package thinqtt;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executor;

public class MQTTDecoder {
	
	public static void decode(InputStream is, final MQTTDecoderListener listener, Executor exec) throws IOException {
		final int fixedHeader = is.read();
		if (fixedHeader == -1) throw new SocketException("EOF on input stream");
		
		// ALGORITHM FOR DECODING REMAINING LENGTH (from MQTT spec)
		// multiplier = 1
		// value = 0
		// do
		//   digit = 'next digit from stream'
		//   value += (digit AND 127) * multiplier
		//   multiplier *= 128
		// while ((digit AND 128) != 0)

		int remainingLength = 0;
		int multiplier = 1;
		int digit;
		do {
			digit = is.read();
			if (digit == -1) throw new SocketException("EOF on input stream");
			remainingLength += (digit & 0x007F) * multiplier;
			multiplier *= 128;
		} while ((digit & 0x0080) != 0);

		final byte[] payload = new byte[remainingLength];
		readFully(is, payload);
		
		if (exec != null) {
			exec.execute(new Runnable() {

				@Override
				public void run() {
					read(fixedHeader, payload, listener);
				}

			});
		}
		
		else {
			read(fixedHeader, payload, listener);
		}
	}

	private static void read(final int fixedHeader, final byte[] payload,
			final MQTTDecoderListener listener) {
		int messageType = (fixedHeader & 0xF0) >> 4;
		boolean dup = (fixedHeader & 0x08) != 0;
		int qos = (fixedHeader & 0x06) >> 1;
		boolean retain = (fixedHeader & 0x01) != 0;
	
		try {
			switch (messageType) {
			case MQTTMessage.CONNECT:
				readConnect(payload, listener);
				break;
			case MQTTMessage.CONNACK:
				readConnAck(payload, listener);
				break;
			case MQTTMessage.PUBLISH:
				readPublish(payload, qos, retain, dup, listener);
				break;
			case MQTTMessage.PUBACK:
				readPubAck(payload, listener);
				break;
			case MQTTMessage.PUBREC:
				readPubRec(payload, listener);
				break;
			case MQTTMessage.PUBREL:
				readPubRel(payload, dup, listener);
				break;
			case MQTTMessage.PUBCOMP:
				readPubComp(payload, listener);
				break;
			case MQTTMessage.SUBSCRIBE:
				readSubscribe(payload, dup, listener);
				break;
			case MQTTMessage.SUBACK:
				readSubAck(payload, listener);
				break;
			case MQTTMessage.UNSUBSCRIBE:
				readUnsubscribe(payload, dup, listener);
				break;
			case MQTTMessage.UNSUBACK:
				readUnsubAck(payload, listener);
				break;
			case MQTTMessage.PINGREQ:
				readPingReq(listener);
				break;
			case MQTTMessage.PINGRESP:
				readPingResp(listener);
				break;
			case MQTTMessage.DISCONNECT:
				readDisconnect(listener);
				break;
			default:
				throw new MQTTClientException("unknown message type: " + messageType);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void readFully(InputStream is, byte[] buffer) throws IOException {
		int start = 0;
		int lengthToRead = buffer.length;
		
		while (lengthToRead > 0) {
			int countRead = is.read(buffer, start, lengthToRead);
			if (countRead < 0) break;
			start = start + countRead;
			lengthToRead = lengthToRead - countRead;
		}
	}

	private static void readDisconnect(MQTTDecoderListener listener) {
		listener.onDisconnect();
	}

	private static void readPingResp(MQTTDecoderListener listener) {
		listener.onPingResp();
	}

	private static void readPingReq(MQTTDecoderListener listener) {
		listener.onPingReq();
	}

	private static void readUnsubAck(byte[] payload, MQTTDecoderListener listener) throws IOException {
		DataInputStream dis = new DataInputStream(
				new ByteArrayInputStream(payload));
		
		int messageId = dis.readShort();
		listener.onUnsubAck(messageId);
	}

	private static void readUnsubscribe(byte[] payload, boolean dup, MQTTDecoderListener listener)
			throws IOException {
		DataInputStream dis = new DataInputStream(
				new ByteArrayInputStream(payload));
		
		int messageId = dis.readShort();
		Collection<String> subList = new ArrayList<String>();
		while (dis.available() > 0) {
			String topic = dis.readUTF();
			subList.add(topic);
		}
		String[] subs = new String[subList.size()];
		subList.toArray(subs);
		listener.onUnsubscribe(messageId, dup, subs);
	}

	private static void readSubAck(byte[] payload, MQTTDecoderListener listener) throws IOException {
		DataInputStream dis = new DataInputStream(
				new ByteArrayInputStream(payload));
		
		int messageId = dis.readShort();
		byte[] qosList = null;
		if (dis.available() > 0) {
			qosList = new byte[(int) dis.available()];
			dis.readFully(qosList);
		}
		listener.onSubAck(messageId, qosList);
	}

	private static void readSubscribe(byte[] payload, boolean dup, MQTTDecoderListener listener)
			throws IOException {
		DataInputStream dis = new DataInputStream(
				new ByteArrayInputStream(payload));
		
		int messageId = dis.readShort();
		Collection<String> subList = new ArrayList<String>();
		while (dis.available() > 0) {
			String topic = dis.readUTF();
			int qos = dis.readByte();
			subList.add("" + qos + topic);
		}
		String[] subs = new String[subList.size()];
		subList.toArray(subs);
		listener.onSubscribe(messageId, dup, subs);
	}

	private static void readPubComp(byte[] payload, MQTTDecoderListener listener) throws IOException {
		DataInputStream dis = new DataInputStream(
				new ByteArrayInputStream(payload));
		
		int messageId = dis.readShort();
		listener.onPubComp(messageId);
	}

	private static void readPubRel(byte[] payload, boolean dup, MQTTDecoderListener listener)
			throws IOException {
		DataInputStream dis = new DataInputStream(
				new ByteArrayInputStream(payload));
		
		int messageId = dis.readShort();
		listener.onPubRel(messageId, dup);
	}

	private static void readPubRec(byte[] payload, MQTTDecoderListener listener) throws IOException {
		DataInputStream dis = new DataInputStream(
				new ByteArrayInputStream(payload));
		
		int messageId = dis.readShort();
		listener.onPubRec(messageId);
	}

	private static void readPubAck(byte[] payload, MQTTDecoderListener listener) throws IOException {
		DataInputStream dis = new DataInputStream(
				new ByteArrayInputStream(payload));
		
		int messageId = dis.readShort();
		listener.onPubAck(messageId);
	}

	private static void readPublish(byte[] payload, int qos, boolean retain,
			boolean dup, MQTTDecoderListener listener) throws IOException {
		DataInputStream dis = new DataInputStream(
				new ByteArrayInputStream(payload));
		
		String topic = dis.readUTF();
		int messageId = 0;
		if (qos > 0) {
			messageId = dis.readShort();
		}

		byte[] message = null;
		if (dis.available() > 0) {
			message = new byte[dis.available()];
			dis.readFully(message);
		} else {
			message = new byte[0];
		}

		listener.onPublish(topic, messageId, message, qos, retain, dup);
	}

	private static void readConnAck(byte[] payload, MQTTDecoderListener listener) throws IOException {
		int responseCode = payload[1];
		listener.onConnAck(responseCode);
	}

	private static void readConnect(byte[] payload, MQTTDecoderListener listener) throws IOException {
		DataInputStream dis = new DataInputStream(
				new ByteArrayInputStream(payload));
		
		String protocol = dis.readUTF();
		int version = dis.readByte();
		byte flags = dis.readByte();

		boolean hasUserName = (flags & 0x80) != 0;
		boolean hasPassword = (flags & 0x40) != 0;
		boolean retainLWT = (flags & 0x20) != 0;
		int lwtQos = (flags & 0x18) >> 3;
		boolean hasLWT = (flags & 0x04) != 0;
		boolean cleanSession = (flags & 0x02) != 0;

		int keepAliveTime = dis.readShort();

		String clientId = dis.readUTF();
		String lwtTopic = hasLWT ? dis.readUTF() : null;
		String lwtMessage = hasLWT ? dis.readUTF() : null;
		String userName = hasUserName && dis.available() > 0 ? dis.readUTF()
				: null;
		String password = hasPassword && dis.available() > 0 ? dis.readUTF()
				: null;

		listener.onConnect(clientId, userName, password, protocol, version,
				hasLWT, lwtMessage, lwtTopic, lwtQos, retainLWT, cleanSession,
				keepAliveTime);
	}

}
