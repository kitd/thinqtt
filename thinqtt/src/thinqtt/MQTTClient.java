package thinqtt;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
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

	private final 	String 			host;
	private final 	int 			port;
	private final 	String 			clientId;
	private final 	MQTTCallback 	cb;
	private final 	MQTTMessageStore store = new MQTTMessageStore();
	private 		Socket 			socket;
	private 		InputStream 	input;
	private 		DataOutputStream output;
	private 		long		 	keepAlive;
	private 		long 			reconnectInterval;
	private 		long 			reconnectIntervalInc;
	private 		long 			reconnectIntervalMax;
	private 		boolean 		active = false;
	private 		long 			lastActivityCheck;
	private final	AtomicInteger 	msgId = new AtomicInteger(0);
	private final	AtomicBoolean	isRunning = new AtomicBoolean(false);
	private final	AtomicBoolean	socketOpen = new AtomicBoolean(false);
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
				if (socketOpen.get()) {
					handleInput(); 
				}
				
				// Try to connect if we aren't connected.
				else {
					handleConnection();
				}
			}
		}
	};

	
	/**
	 * Public API methods
	 */

	/**
	 * 
	 * @param host hostname of MQTT server
	 * @param port port of MQTT server
	 * @param clientId ID of this client
	 * @param workPool a thread pool for decoding tasks, or null if synchronous required
	 * @param outPool a single-threaded ThreadPoolExecutor for socket output tasks, or null if synchronous required
	 * @param listener the callback to notify of events
	 */
	public MQTTClient(String host, int port, String clientId, Executor workPool, MQTTCallback listener) {
		if (log.isLoggable(Level.FINER)) {
			log.entering(getClass().getName(), "<INIT>", new Object[]{host, port, clientId});
		}
		
		// Validate arguments
		if (host == null || host.trim().length() == 0)
			throw new IllegalArgumentException("Host name cannot be null or empty.");
		this.host = host;

		if (port < 1 || port > 65535)
			throw new IllegalArgumentException("Port must be >= 0 and <= 65535.");
		this.port = port;

		this.clientId = clientId == null ? generateRandomId() : clientId.trim();
		if (this.clientId.length() == 0	|| this.clientId.length() > 23)
			throw new IllegalArgumentException("Client ID cannot be empty or more than 23 characters.");

		if (listener == null)
			throw new IllegalArgumentException("Listener cannot be null.");
		this.cb = listener;
		
		this.workQ = workPool;

		log.exiting(getClass().getName(), "<INIT>");
	}

	public MQTTClient(String host, int port, String clientId, MQTTCallback listener) {
		this(host, port, clientId, null, listener);
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

		isRunning.set(false);
		writeQ.execute(doDisconnect());
		active = true;

		log.exiting(getClass().getName(), "disconnect");
	}

	public void subscribe(final String topicPattern, final int qos)
			throws IOException {
		int msgId = nextMessageId();
		store.put(MQTTMessage.SUBSCRIBE, msgId, qos, topicPattern, null);
		writeQ.execute(doSubscribe(topicPattern, qos, msgId));
		active = true;
	}

	public int publish(final String topic, final byte[] message, final int qos)
			throws IOException {
		int msgId = qos > 0 ? nextMessageId() : 0;

		if (qos > 0) {
			store.put(MQTTMessage.PUBLISH, msgId, qos, topic, message);
		}

		writeQ.execute(doPublish(topic, message, qos, msgId));

		active = true;
		return msgId;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getClientId() {
		return clientId;
	}

	public boolean isNetworkConnected() {
		return socket != null && !socket.isClosed();
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
	protected void onConnAck(int responseCode) throws MQTTClientException {
		active = true;

		if (responseCode == 0) {
			log.info("Connected to " + this.host + ":" + this.port + " with ID " + this.clientId);
			cb.onConnected();
		}

		else {
			Exception e = new MQTTClientException(responseCode > 0 && responseCode <= 5 
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
			store.put(MQTTMessage.PUBACK, messageId, qos, topic, payload);
			cb.messageArrived(topic, payload);
			store.delete(messageId);
			writeQ.execute(doPubAck(messageId));
			break;
		case 2:
			store.put(MQTTMessage.PUBREC, messageId, qos, topic, payload);
			writeQ.execute(doPubRec(messageId));
			break;
		default:
			cb.errorOccurred(new MQTTClientException(MQTT_INVALID_QOS + qos));
		}

		// if (retain) {
		// retainedMsg = new MQTTMessage(messageId, topic, payload);
		// }

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
					MQTTEncoder.writeConnect(getDataOutputStream(), clientId, user, password, lwtTopic,
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
					MQTTEncoder.writeDisconnect(getDataOutputStream());
					socket.close();
					socket = null;
					socketOpen.set(false);
					writeQ.shutdown();
				} catch (IOException e) {
					handleSocketError(e);
				} 
			}
		};
	}

	private Runnable doSubscribe(final String topicPattern, final int qos,
			final int msgId) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					MQTTEncoder.writeSubscribe(getDataOutputStream(), msgId, topicPattern, qos);
				} catch (IOException e) {
					handleSocketError(e);
				}
			}
		};
	}

	private Runnable doPublish(final String topic, final byte[] message,
			final int qos, final int msgId) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					MQTTEncoder.writePublish(getDataOutputStream(), topic, message, msgId, qos, false);
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
					MQTTEncoder.writePubComp(getDataOutputStream(), messageId);
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
					MQTTEncoder.writePubRec(getDataOutputStream(), messageId);
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
					MQTTEncoder.writePubRel(getDataOutputStream(), messageId);
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
					MQTTEncoder.writePubAck(getDataOutputStream(), messageId);
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
					MQTTEncoder.writePing(getDataOutputStream());
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
		socket.connect(new InetSocketAddress(host, port));
		
		input = new BufferedInputStream(socket.getInputStream());
		output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
		socketOpen.set(true);
		
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
			if (!active && socketOpen.get()) {
				writeQ.execute(doPing());
			}
			lastActivityCheck = now;
			active = false;
		}
	}

	private DataOutputStream getDataOutputStream() {
		return output;
	}

	private void handleSocketError(Exception e) {
		socketOpen.set(false);
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

	private void handleConnection() {
		try {
			openConnection(connectProps);
		} catch (IOException e) {
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
