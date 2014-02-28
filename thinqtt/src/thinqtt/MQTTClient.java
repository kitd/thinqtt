package thinqtt;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;

public class MQTTClient extends MQTTDecoderListener {
	private static final int READ_LOOP_INTERVAL = 250;
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

	private final String host;
	private final int port;
	private final String clientId;

	private final MQTTCallback cb;
	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
	private final ExecutorService workQ;
	private final ExecutorService writeQ = Executors.newSingleThreadExecutor();
	private final MQTTMessageStore store = new MQTTMessageStore();

	private Socket socket;
	private MQTTDecoder decoder;
	private MQTTEncoder encoder;
	private long keepAlive;
	private boolean connected = false;
	private boolean active = false;
	private long lastActivityCheck;
	private boolean pinging = false;
	private final AtomicInteger msgId = new AtomicInteger(0);
	private final AtomicBoolean running = new AtomicBoolean(true);
	// private MQTTMessage retainedMsg = null;

	private final Runnable readLoop = new Runnable() {
		@Override
		public void run() {
			while (running.get()) {
				try {
					while (decoder.canDecode()) {
						decoder.decode();
					}
					
					pingLoop.run();
					
					Thread.sleep(READ_LOOP_INTERVAL);
				} catch (SocketException se) {
					checkConnection();
				} catch (IOException e) {
					cb.errorOccurred(e);
					checkConnection();
				} catch (InterruptedException e) {
				}
			}
		}
	};

	private final Runnable pingLoop = new Runnable() {
		@Override
		public void run() {
			long now = System.currentTimeMillis(); 
			if (now - lastActivityCheck > keepAlive) {
				// Time for an activity check
				if (!active) {
					// If we are already pinging, we haven't had a 
					// reply from the previous ping, so something's
					// wrong.
					if (pinging) {
						checkConnection();
					} else {
						// If we get here, we are inactive and we
						// need to ping.
						pinging = true;
						writeQ.submit(doPing());
					}
				}
				lastActivityCheck = now;
				active = false;
			}
		}
	};
	
	/**
	 * Public API methods
	 */

	public MQTTClient(String host, int port, String clientId, ExecutorService async, MQTTCallback listener) {
		if (host == null || host.trim().length() == 0)
			throw new IllegalArgumentException(
					"Host name cannot be null or empty.");
		this.host = host;

		if (port < 0 || port > 65535)
			throw new IllegalArgumentException(
					"Port cannot be >= 0 and <= 65535.");
		this.port = port;

		this.clientId = clientId == null ? generateRandomId() : clientId;
		if (this.clientId.trim().length() == 0
				|| this.clientId.trim().length() > 23)
			throw new IllegalArgumentException(
					"Client ID cannot be null, empty or more than 23 characters.");

		if (listener == null)
			throw new IllegalArgumentException("Listener cannot be null.");
		this.cb = listener;

		this.workQ = async != null ? async : Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

	}

	public void connect() throws UnknownHostException, IOException {
		connect(new Properties());
	}

	public void connect(Properties connectionProperties)
			throws UnknownHostException, IOException {
		
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
				"keepAliveSecs", "60")) * 1000;

		socket = SocketFactory.getDefault().createSocket();
		socket.setReceiveBufferSize(DEFAULT_BUFFER_SIZE);
		socket.connect(new InetSocketAddress(host, port));
		
		InputStream in = new BufferedInputStream(socket.getInputStream());
		OutputStream out = new BufferedOutputStream(socket.getOutputStream());
		this.decoder = new MQTTDecoder(in, workQ, this);
		this.encoder = new MQTTEncoder(out);

		writeQ.submit(doConnect(user, password, lwtTopic, lwtMsg, lwtQos,
				lwtRetain, cleanSession));

		running.set(true);
		new Thread(readLoop).start();
		lastActivityCheck = System.currentTimeMillis();
//		scheduler.scheduleWithFixedDelay(pingLoop, 10, 10, TimeUnit.SECONDS);
	}

	public void disconnect() {
		if (connected) {
			running.set(false);
			scheduler.shutdown();
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

	public boolean isConnected() {
		return connected;
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
			connected = true;
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
		connected = true;
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
			writeQ.submit(doPubAck(messageId));
			store.delete(messageId);
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
					connected = false;
					socket.close();
					writeQ.shutdown();
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

	private void checkConnection() {
		connected = isNetworkConnected();
		if (!connected) {
			cb.connectionLost();
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

}
