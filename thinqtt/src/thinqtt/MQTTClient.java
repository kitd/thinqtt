package thinqtt;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.SocketFactory;

public class MQTTClient extends MQTTDecoderListener {
	private static final int DEFAULT_BUFFER_SIZE = 512 * 1024;
	
	public static final String[] CONNECTION_ERRMSG = new String[] {
			"Connection Refused: unacceptable protocol version",
			"Connection Refused: identifier rejected",
			"Connection Refused: server unavailable",
			"Connection Refused: bad user name or password",
			"Connection Refused: not authorized",
			"Connection Refused: unknown reason code " };
	public static final String MQTT_INVALID_QOS = "Unknown QoS code ";
	private static final String ALPHANUM_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	
	private static Logger log = Logger.getLogger(MQTTClient.class.getName());

	private final 	String 			host;
	private final 	int 			port;
	private final 	String 			clientId;
	private final 	MQTTCallback 	cb;
	private final 	MQTTMessageStore store = new MQTTMessageStore();
	private 		Socket 			socket;
	private 		MQTTDecoder 	decoder;
	private 		MQTTEncoder 	encoder;
	private 		long		 	keepAlive;
	private 		boolean 		active = false;
	private 		long 			lastActivityCheck;
	private final	AtomicInteger 	msgId = new AtomicInteger(0);
	private final	AtomicBoolean	isRunning = new AtomicBoolean(false);
	private final   Timer			pinger = new Timer();
	private final 	ExecutorService workQ;
	private final 	ExecutorService writeQ = Executors.newSingleThreadExecutor();
	
	private final 	Thread 			reader = new Thread(new Runnable() {
		@Override
		public void run() {
			while (isRunning.get()) {
				try {
					decoder.decode();
				} catch (SocketTimeoutException ste) {
					continue;
				} catch (SocketException se) {
					if (!isNetworkConnected()) {
						break;
					}
				} catch (Exception e) {
					cb.errorOccurred(e);
					if (!checkConnection()) {
						break;
					}
				}
			}
		}
	});
	
	/**
	 * Public API methods
	 */

	public MQTTClient(String host, int port, String clientId, ExecutorService async, MQTTCallback listener) {
		if (log.isLoggable(Level.FINER)) {
			log.entering(getClass().getName(), "<INIT>", new Object[]{host, port, clientId, async, listener});
		}
		
		if (host == null || host.trim().length() == 0)
			throw new IllegalArgumentException(
					"Host name cannot be null or empty.");
		this.host = host;

		if (port < 0 || port > 65535)
			throw new IllegalArgumentException(
					"Port cannot be >= 0 and <= 65535.");
		this.port = port;

		this.clientId = clientId == null ? generateRandomId() : clientId.trim();
		if (this.clientId.length() == 0	|| this.clientId.length() > 23)
			throw new IllegalArgumentException(
					"Client ID cannot be null, empty or more than 23 characters.");

		if (listener == null)
			throw new IllegalArgumentException("Listener cannot be null.");
		this.cb = listener;

		this.workQ = async != null ? async : Executors.newFixedThreadPool(
				Runtime.getRuntime().availableProcessors());

		log.exiting(getClass().getName(), "<INIT>");
	}

	public void connect() throws UnknownHostException, IOException {
		connect(new Properties());
	}

	public void connect(Properties connectionProperties)
			throws UnknownHostException, IOException {
		if (log.isLoggable(Level.FINER)) {
			log.entering(getClass().getName(), "connect", connectionProperties.toString());
		}
		
		String user = connectionProperties.getProperty("user");
		String password = connectionProperties.getProperty("password");
		String lwtTopic = connectionProperties.getProperty("lwtTopic");
		String lwtMsg = connectionProperties.getProperty("lwtMsg", clientId
				+ " is offline");
		int lwtQos = Integer.parseInt(connectionProperties.getProperty(
				"lwtQos", "0"));
		boolean lwtRetain = Boolean.parseBoolean(connectionProperties
				.getProperty("lwtRetain", "False"));
		boolean cleanSession = Boolean.parseBoolean(connectionProperties
				.getProperty("cleanSession", "False"));
		this.keepAlive = Integer.parseInt(connectionProperties.getProperty(
				"keepAliveSecs", "60")) * 1000; // KeepAlive stored as millis

		socket = SocketFactory.getDefault().createSocket();
		socket.setReceiveBufferSize(DEFAULT_BUFFER_SIZE);
		socket.setSoTimeout(5000);
		socket.connect(new InetSocketAddress(host, port));
		
		InputStream in = new BufferedInputStream(socket.getInputStream());
		OutputStream out = new BufferedOutputStream(socket.getOutputStream());
		this.decoder = new MQTTDecoder(in, workQ, this);
		this.encoder = new MQTTEncoder(out);

		writeQ.submit(doConnect(user, password, lwtTopic, lwtMsg, lwtQos,
				lwtRetain, cleanSession));

		lastActivityCheck = System.currentTimeMillis();
		isRunning.set(true);
		reader.start();
		pinger.schedule(new TimerTask() {
			@Override
			public void run() {
				checkActivity();
			}
		}, 10000, 10000);
		log.exiting(getClass().getName(), "connect");
	}

	public void disconnect() {
		if (isNetworkConnected()) {
			isRunning.set(false);
//			readLoop.interrupt();
			pinger.cancel();
			workQ.shutdown();
			writeQ.submit(doDisconnect());
			recordActivity();
		}
	}

	public void subscribe(final String topicPattern, final int qos)
			throws IOException {
		int msgId = nextMessageId();
		store.put(MQTTMessage.SUBSCRIBE, msgId, qos, topicPattern, null);
		writeQ.submit(doSubscribe(topicPattern, qos, msgId));
		recordActivity();
	}

	public int publish(final String topic, final byte[] message, final int qos)
			throws IOException {
		int msgId = qos > 0 ? nextMessageId() : 0;

		if (qos > 0) {
			store.put(MQTTMessage.PUBLISH, msgId, qos, topic, message);
		}

		writeQ.submit(doPublish(topic, message, qos, msgId));

		recordActivity();
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

		if (responseCode == 0) {
			cb.onConnected();
			recordActivity();
		}

		else {
			cb.errorOccurred(new MQTTClientException(responseCode > 0
					&& responseCode < 5 ? CONNECTION_ERRMSG[responseCode - 1]
					: CONNECTION_ERRMSG[5] + responseCode));
			checkConnection();
		}
	}

	@Override
	protected void onPingResp() {
		recordActivity();
	}

	@Override
	protected void onUnsubAck(int messageId) {
		super.onUnsubAck(messageId);
		recordActivity();
	}

	@Override
	protected void onSubAck(int messageId, byte[] qosList) {
		store.delete(messageId);
		recordActivity();
	}

	@Override
	protected void onPublish(String topic, final int messageId, byte[] payload,
			int qos, boolean retain, boolean dup) {
		switch (qos) {
		case 0:
			cb.messageArrived(topic, payload);
			break;
		case 1:
			store.put(MQTTMessage.PUBACK, messageId, qos, topic, payload);
			cb.messageArrived(topic, payload);
			store.delete(messageId);
			writeQ.submit(doPubAck(messageId));
			break;
		case 2:
			store.put(MQTTMessage.PUBREC, messageId, qos, topic, payload);
			writeQ.submit(doPubRec(messageId));
			break;
		default:
			cb.errorOccurred(new MQTTClientException(MQTT_INVALID_QOS + qos));
		}

		// if (retain) {
		// retainedMsg = new MQTTMessage(messageId, topic, payload);
		// }
		recordActivity();

	}

	@Override
	protected void onPubComp(int messageId) {
		store.delete(messageId);
		cb.publishComplete(messageId);
		recordActivity();
	}

	@Override
	protected void onPubAck(int messageId) {
		store.delete(messageId);
		cb.publishComplete(messageId);
		recordActivity();
	}

	@Override
	protected void onPubRec(final int messageId) {
		if (store.contains(messageId)) {
			MQTTMessage msg = store.get(messageId);
			writeQ.submit(doPubRel(messageId));
		}
		recordActivity();
	}

	@Override
	protected void onPubRel(final int messageId, boolean dup) {
		if (store.contains(messageId)) {
			MQTTMessage msg = store.get(messageId);
			cb.messageArrived(msg.getTopic(), msg.getMsg());
			writeQ.submit(doPubComp(messageId));
			store.delete(messageId);
		}
		recordActivity();
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
					encoder.writeConnect(clientId, user, password, lwtTopic,
							lwtMsg, lwtQos, lwtRetain, cleanSession, (int) (keepAlive/1000));
				} catch (IOException e) {
					cb.errorOccurred(e);
					checkConnection();
				}
			}
		};
	}

	private Runnable doDisconnect() {
		return new Runnable() {
			@Override
			public void run() {
				try {
					encoder.writeDisconnect();
					writeQ.shutdown();
					socket.close();
				} catch (IOException e) {
					cb.errorOccurred(e);
					checkConnection();
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
					encoder.writeSubscribe(msgId, topicPattern, qos);
				} catch (IOException e) {
					cb.errorOccurred(e);
					checkConnection();
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
					encoder.writePublish(topic, message, msgId, qos);
				} catch (IOException e) {
					cb.errorOccurred(e);
					checkConnection();
				}
			}
		};
	}

	private Runnable doPubComp(final int messageId) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					encoder.writePubComp(messageId);
				} catch (IOException e) {
					cb.errorOccurred(e);
					checkConnection();
				}
			}
		};
	}

	private Runnable doPubRec(final int messageId) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					encoder.writePubRec(messageId);
				} catch (IOException e) {
					cb.errorOccurred(e);
					checkConnection();
				}
			}
		};
	}

	private Runnable doPubRel(final int messageId) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					encoder.writePubRel(messageId);
				} catch (IOException e) {
					cb.errorOccurred(e);
					checkConnection();
				}
			}
		};
	}

	private Runnable doPubAck(final int messageId) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					encoder.writePubAck(messageId);
				} catch (IOException e) {
					cb.errorOccurred(e);
					checkConnection();
				}
			}
		};
	}

	private Runnable doPing() {
		return new Runnable() {
			@Override
			public void run() {
				try {
					encoder.writePing();
				} catch (IOException e) {
					cb.errorOccurred(e);
					checkConnection();
				}
			}
		};
	}

	private boolean checkConnection() {
		if (!isNetworkConnected()) {
			cb.connectionLost();
			return false;
		}
		
		else {
			return true;
		}
	}

	private int nextMessageId() {
		return msgId.incrementAndGet();
	}

	private void recordActivity() {
		active = true;
	}

	private String generateRandomId() {
		Random r = new Random();
		char[] result = new char[20];
		for (int i = 0; i < 20; i++) {
			result[i] = ALPHANUM_CHARS
					.charAt(r.nextInt(ALPHANUM_CHARS.length()));
		}
		return new String(result);
	}

	private void checkActivity() {
		long now = System.currentTimeMillis(); 
		if (now - lastActivityCheck > keepAlive) {
			// Time to check for activity
			if (!active && checkConnection()) {
				writeQ.submit(doPing());
			}
			lastActivityCheck = now;
			active = false;
		}
	}

}
