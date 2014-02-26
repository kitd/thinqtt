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
	
	private final int id;
	private final String topic;
	private final byte[] msg;
	
	public MQTTMessage(int id, String topic, byte[] msg) {
		this.id = id;
		this.topic = topic;
		this.msg = msg;
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

}
