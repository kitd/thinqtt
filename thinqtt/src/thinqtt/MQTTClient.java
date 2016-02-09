package thinqtt;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.SocketFactory;

public class MQTTClient extends MQTTDecoderListener {
	
	private static final int SOCKET_TIMEOUT = 5000;
	private static final int DEFAULT_BUFFER_SIZE = 512 * 1024;
	
	public static final String[] CONNECTION_ERRMSG = new String[] {
			"Connection Refused: unacceptable protocol version",
			"Connection Refused: identifier rejected",
			"Connection Refused: server unavailable",
			"Connection Refused: bad user name or password",
			"Connection Refused: not authorized",
			"Connection Refused: unknown reason code " };
	public static final String MQTT_INVALID_QOS = "Unknown QoS code ";
	private static final char[] ALPHANUM_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
	
	private static Logger log = Logger.getLogger(MQTTClient.class.getName());

	private final 	URI 			uri;
	private final 	String 			clientId;
	private final 	MQTTCallback 	cb;
	private final 	MQTTMessageStore store = new MQTTMessageStore();
	private 		MQTTMessage		retainedMsg;
	private 		Socket 			socket;
	private 		DataInputStream input;
	private 		DataOutputStream output;
	private 		long		 	keepAlive;
	private 		long 			reconnectInterval;
	private 		long 			reconnectIntervalInc;
	private 		long 			reconnectIntervalMax;
	private 		boolean 		active = false;
	private 		long 			lastActivityCheck;
	private final	AtomicInteger 	msgId = new AtomicInteger(0);
	private final	AtomicBoolean	isRunning = new AtomicBoolean(false);
	// workQ is a thread pool that handles decoding tasks. 
	// Calls to the MQTTCallback will be made on one of these threads.
	private final 	Executor 		workQ;
	private 	 	ExecutorService	writeQ = Executors.newSingleThreadExecutor();
	private 		Properties      connectProps;

	// Main loop (which runs on its own thread). 
	// Handles reading input OR connecting to the server depending 
	// on the state of the socket.
	private final 	Runnable		reader = new Runnable() {
		@Override
		public void run() {
			while (isRunning.get()) {
				if (socket != null) {
					handleInput(); 
				}
				
				// Try to reconnect if we aren't connected.
				else {
					handleReconnection();
				}
			}
		}
	};

	
	/**
	 * Public API methods
	 */

	/**
	 * 
	 * @param uri uri of MQTT server
	 * @param clientId ID of this client
	 * @param workPool a thread pool for decoding tasks, or null if synchronous required
	 * @param outPool a single-threaded ThreadPoolExecutor for socket output tasks, or null if synchronous required
	 * @param listener the callback to notify of events
	 */
	public MQTTClient(URI uri, String clientId, Executor workPool, MQTTCallback listener) {
		if (log.isLoggable(Level.FINER)) {
			log.entering(getClass().getName(), "<INIT>", uri.toASCIIString());
		}
		
		// Validate arguments
		if (uri == null)
			throw new IllegalArgumentException("URI cannot be null.");
		this.uri = uri;

		this.clientId = clientId == null ? generateRandomId() : clientId.trim();
		if (this.clientId.length() == 0	|| this.clientId.length() > 23)
			throw new IllegalArgumentException("Client ID cannot be empty or more than 23 characters.");

		if (listener == null)
			throw new IllegalArgumentException("Listener cannot be null.");
		this.cb = listener;
		
		this.workQ = workPool;

		log.exiting(getClass().getName(), "<INIT>");
	}

	public MQTTClient(URI uri, String clientId, MQTTCallback listener) {
		this(uri, clientId, null, listener);
	}
	
	public void connect() throws UnknownHostException, IOException {
		connect(new Properties());
	}

	public void connect(Properties connectionProperties)
			throws UnknownHostException, IOException {
		log.entering(getClass().getName(), "connect", connectionProperties.toString());
		
		this.connectProps = connectionProperties;
		openConnection(this.connectProps);

		// Start the main loop.
		isRunning.set(true);
		new Thread(reader).start();
		
		// Start the activity check task
		lastActivityCheck = System.currentTimeMillis();
		
		log.exiting(getClass().getName(), "connect");
	}

	public void disconnect() {
		log.entering(getClass().getName(), "disconnect");

		try {
			isRunning.set(false);
			socket.shutdownInput();
			writeQ.execute(doDisconnect());
		} catch (Exception e) {}
		active = true;

		log.exiting(getClass().getName(), "disconnect");
	}

	public void subscribe(final String topicPattern, final int qos)
			throws IOException {
		int msgId = nextMessageId();
		store.put(MQTTMessage.SUBSCRIBE, msgId, qos, topicPattern, null, false);
		writeQ.execute(doSubscribe(topicPattern, qos, msgId));
		active = true;
	}

	public int publish(final String topic, final byte[] message, final int qos, final boolean retained)
			throws IOException {
		int msgId = qos > 0 ? nextMessageId() : 0;

		if (qos > 0) {
			store.put(MQTTMessage.PUBLISH, msgId, qos, topic, message, retained);
		}

		writeQ.execute(doPublish(topic, message, qos, msgId, retained));

		active = true;
		return msgId;
	}

	public URI getUri() {
		return uri;
	}

	public String getClientId() {
		return clientId;
	}

	public boolean isConnected() {
		return socket != null;
	}

	public int getPendingMessageCount() {
		return store.count();
	}

	/**************************************************************
	 * on... methods.
	 * 
	 * These implement the MQTTDecodeListener interface and handle the decoded
	 * MQTT messages received on the socket input stream
	 */

	@Override
	protected void onConnAck(int responseCode) throws MQTTException {
		active = true;

		if (responseCode == 0) {
			log.info("Connected to " + this.uri.toASCIIString() + " with ID " + this.clientId);
			cb.onConnected();
		}

		else {
			Exception e = new MQTTException(responseCode > 0 && responseCode <= 5 
					? CONNECTION_ERRMSG[responseCode - 1]
					: CONNECTION_ERRMSG[5] + responseCode);
			cb.errorOccurred(e);
			log.severe(e.getMessage());
		}
	}
	
	

	@Override
	protected void onDisconnect() {
		cb.errorOccurred(new IllegalStateException("Server sent disconnect"));
	}

	@Override
	protected void onPingResp() {
		active = true;
	}

	@Override
	protected void onUnsubAck(int messageId) {
		active = true;
		super.onUnsubAck(messageId);
	}

	@Override
	protected void onSubAck(int messageId, byte[] qosList) {
		active = true;
		store.delete(messageId);
	}

	@Override
	protected void onPublish(String topic, final int messageId, byte[] payload,
			int qos, boolean retain, boolean dup) {
		active = true;
		
		switch (qos) {
		case 0:
			cb.messageArrived(topic, payload);
			break;
		case 1:
			store.put(MQTTMessage.PUBACK, messageId, qos, topic, payload, retain);
			cb.messageArrived(topic, payload);
			store.delete(messageId);
			writeQ.execute(doPubAck(messageId));
			break;
		case 2:
			store.put(MQTTMessage.PUBREC, messageId, qos, topic, payload, retain);
			writeQ.execute(doPubRec(messageId));
			break;
		default:
			cb.errorOccurred(new MQTTException(MQTT_INVALID_QOS + qos));
		}

		if (retain) {
            retainedMsg = new MQTTMessage(MQTTMessage.PUBLISH, messageId, qos, topic, payload, true);
		}

	}

	@Override
	protected void onPubComp(int messageId) {
		active = true;
		store.delete(messageId);
		cb.publishComplete(messageId);
	}

	@Override
	protected void onPubAck(int messageId) {
		active = true;
		store.delete(messageId);
		cb.publishComplete(messageId);
	}

	@Override
	protected void onPubRec(final int messageId) {
		active = true;
		if (store.contains(messageId)) {
//			MQTTMessage msg = store.get(messageId);
			writeQ.execute(doPubRel(messageId));
		}
	}

	@Override
	protected void onPubRel(final int messageId, boolean dup) {
		active = true;
		if (store.contains(messageId)) {
			MQTTMessage msg = store.get(messageId);
			cb.messageArrived(msg.getTopic(), msg.getMsg());
			writeQ.execute(doPubComp(messageId));
			store.delete(messageId);
		}
	}

	/********************************************************
	 * do... methods
	 * 
	 * These return a Runnable for asynchronously calling the encoder to output
	 * an MQTT message.
	 */

	private Runnable doConnect(final String user, final String password,
			final String lwtTopic, final String lwtMsg, final int lwtQos,
			final boolean lwtRetain, final boolean cleanSession) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					MQTTEncoder.writeConnect(output, clientId, user, password, lwtTopic,
							lwtMsg, lwtQos, lwtRetain, cleanSession, (int) (keepAlive/1000));
				} catch (IOException e) {
					handleSocketError(e);
				}
			}
		};
	}

	private Runnable doDisconnect() {
		return new Runnable() {
			@Override
			public void run() {
				try {
					MQTTEncoder.writeDisconnect(output);
				} catch (IOException e) {
					handleSocketError(e);
				} 
				
				try {
					socket.close();
				} catch (IOException e) {}
				
				socket = null;
				writeQ.shutdown();
				cb.onDisconnected();
			}
		};
	}

	private Runnable doSubscribe(final String topicPattern, final int qos,
			final int msgId) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					MQTTEncoder.writeSubscribe(output, msgId, topicPattern, qos);
				} catch (IOException e) {
					handleSocketError(e);
				}
			}
		};
	}

	private Runnable doPublish(final String topic, final byte[] message,
			final int qos, final int msgId, final boolean retained) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					MQTTEncoder.writePublish(output, topic, message, msgId, qos, retained);
				} catch (IOException e) {
					handleSocketError(e);
				}
			}
		};
	}

	private Runnable doPubComp(final int messageId) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					MQTTEncoder.writePubComp(output, messageId);
				} catch (IOException e) {
					handleSocketError(e);
				}
			}
		};
	}

	private Runnable doPubRec(final int messageId) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					MQTTEncoder.writePubRec(output, messageId);
				} catch (IOException e) {
					handleSocketError(e);
				}
			}
		};
	}

	private Runnable doPubRel(final int messageId) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					MQTTEncoder.writePubRel(output, messageId);
				} catch (IOException e) {
					handleSocketError(e);
				}
			}
		};
	}

	private Runnable doPubAck(final int messageId) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					MQTTEncoder.writePubAck(output, messageId);
				} catch (IOException e) {
					handleSocketError(e);
				}
			}
		};
	}

	private Runnable doPing() {
		return new Runnable() {
			@Override
			public void run() {
				try {
					MQTTEncoder.writePing(output);
				} catch (IOException e) {
					handleSocketError(e);
				}
			}
		};
	}
	
	/* ====================================================
	 * Private helper methods follow ... 
	 * ====================================================
	 */
	
	private void openConnection(Properties connectionProperties) throws IOException, SocketException {
		if (socket != null) {
			try {
				socket.close();
			} catch (Exception e) {}
		}
		// Set up the TCP connection
		socket = SocketFactory.getDefault().createSocket();
		socket.setReceiveBufferSize(DEFAULT_BUFFER_SIZE);
		socket.setSoTimeout(SOCKET_TIMEOUT);
		int port = uri.getPort() == -1 ? 1883 : uri.getPort();
		socket.connect(new InetSocketAddress(uri.getHost(), port));
		
		input = new DataInputStream(
				new BufferedInputStream(
						socket.getInputStream()
						)
				);
		output = new DataOutputStream(
				new BufferedOutputStream(
						socket.getOutputStream()
						)
				);
		
		// Save our properties
		String user = connectionProperties.getProperty("user");
		String password = connectionProperties.getProperty("password");
		String lwtTopic = connectionProperties.getProperty("lwtTopic");
		String lwtMsg = connectionProperties.getProperty("lwtMsg", "MQTT client " + clientId + " is offline");
		int lwtQos = Integer.parseInt(connectionProperties.getProperty("lwtQos", "0"));
		boolean lwtRetain = Boolean.parseBoolean(connectionProperties.getProperty("lwtRetain", "False"));
		boolean cleanSession = Boolean.parseBoolean(connectionProperties.getProperty("cleanSession", "False"));
		// KeepAlive is stored as millis
		this.keepAlive = Integer.parseInt(connectionProperties.getProperty("keepAliveSecs", "60")) * 1000; 
		this.reconnectIntervalInc = Integer.parseInt(connectionProperties.getProperty("reconnectIntervalInc", "3")) * 1000;
		this.reconnectIntervalMax = Integer.parseInt(connectionProperties.getProperty("reconnectIntervalMax", "120")) * 1000;

		// Send the CONNECT msg
		writeQ.execute(doConnect(user, password, lwtTopic, lwtMsg, lwtQos,
				lwtRetain, cleanSession));

	}

	/**
	 * @return the next integer in an incrementing count.
	 */
	private int nextMessageId() {
		return msgId.incrementAndGet();
	}

	/**
	 * @return a 20-character String of random alphanumeric characters 
	 */
	private String generateRandomId() {
		Random r = new Random();
		char[] result = new char[20];
		for (int i = 0; i < 20; i++) {
			result[i] = ALPHANUM_CHARS[r.nextInt(ALPHANUM_CHARS.length)];
		}
		return new String(result);
	}

	
	/**
	 * If it has been {keepAlive} milliseconds since the last call
	 * and we have not recently had activity and we are still connected,
	 * send a ping message to remote MQTT endpoint. 
	 */
	private void checkActivity() {
		long now = System.currentTimeMillis(); 
		if (now - lastActivityCheck > keepAlive) {
			// Time to check for activity
			if (!active && isConnected()) {
				writeQ.execute(doPing());
			}
			lastActivityCheck = now;
			active = false;
		}
	}

	private void handleSocketError(Exception e) {
		socket = null;
		this.reconnectInterval = 0L;
		if (isRunning.get()) {
			log.severe(e.getMessage());
			cb.errorOccurred(e);
			cb.connectionLost();
		}
	}

	private void handleInput() {
		try {
			MQTTDecoder.decode(input, this, workQ);
		} catch (SocketTimeoutException ste) {
			checkActivity();
		} catch (IOException ioe) {
			handleSocketError(ioe);
		}
	}

	private void handleReconnection() {
		try {
			openConnection(connectProps);
		} catch (IOException e) {
			socket = null;
			if (log.isLoggable(Level.FINE)) {
				log.fine(e.getMessage());
			}
			try {
				if (this.reconnectInterval < this.reconnectIntervalMax) {
					this.reconnectInterval += this.reconnectIntervalInc;
				}
				Thread.sleep(this.reconnectInterval);
			} catch (InterruptedException e1) { /* nop */ }
		}
	}
}
