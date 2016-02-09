package thinqtt;



public class MQTTMessage {
	public static final int RESERVED0 	= 0;
	public static final int CONNECT 	= 1;
	public static final int CONNACK 	= 2;
	public static final int PUBLISH 	= 3;
	public static final int PUBACK 		= 4;
	public static final int PUBREC 		= 5;
	public static final int PUBREL 		= 6;
	public static final int PUBCOMP 	= 7;
	public static final int SUBSCRIBE 	= 8;
	public static final int SUBACK 		= 9;
	public static final int UNSUBSCRIBE = 10;
	public static final int UNSUBACK 	= 11;
	public static final int PINGREQ 	= 12;
	public static final int PINGRESP 	= 13;
	public static final int DISCONNECT 	= 14;
	public static final int RESERVED15 	= 15;
	
	private final int type;
	private final int id;
	private final int qos;
	private final String topic;
	private final byte[] msg;
	private final long time;
	private final boolean retain;
	private int retries;
	
	public MQTTMessage(int type, int id, int qos, String topic, byte[] msg, boolean retain) {
		this.type = type;
		this.id = id;
		this.qos = qos;
		this.topic = topic;
		this.msg = msg;
		this.time = System.currentTimeMillis();
		this.retries = 0;
		this.retain = retain;
	}

	public boolean isRetained() {
		return retain;
	}

	public int getQos() {
		return qos;
	}

	public int getType() {
		return type;
	}

	public int getId() {
		return id;
	}

	public String getTopic() {
		return topic;
	}

	public byte[] getMsg() {
		return msg;
	}

	public long getTime() {
		return time;
	}

}
