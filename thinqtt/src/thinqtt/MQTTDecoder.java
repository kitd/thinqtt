package thinqtt;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executor;

public class MQTTDecoder {
	private final DataInputStream dis;
	private final Executor exec;
	private final MQTTDecoderListener listener;

	public MQTTDecoder(InputStream is, Executor exec,
			MQTTDecoderListener listener) {
		this.dis = new DataInputStream(is);
		this.exec = exec;
		this.listener = listener;
	}

	public void decode() throws IOException {
		int fixedHeader = dis.read();

		final int messageType = (fixedHeader & 0xF0) >> 4;
		final boolean dup = (fixedHeader & 0x08) != 0;
		final int qos = (fixedHeader & 0x04) >> 1;
		final boolean retain = (fixedHeader & 0x01) != 0;

		// ALGORITHM FOR DECODING REMAINING LENGTH (from MQTT spec)
		// multiplier = 1
		// value = 0
		// do
		// digit = 'next digit from stream'
		// value += (digit AND 127) * multiplier
		// multiplier *= 128
		// while ((digit AND 128) != 0)

		int remainingLength = 0;
		int multiplier = 1;
		int digit;
		do {
			digit = dis.read();
			remainingLength += (digit & 0x7F) * multiplier;
			multiplier *= 128;
		} while ((digit & 0x80) != 0);

		final byte[] remainder = new byte[remainingLength];
		dis.read(remainder);

		exec.execute(new Runnable() {

			@Override
			public void run() {
				DataInputStream dis = new DataInputStream(
						new ByteArrayInputStream(remainder));

				try {
					switch (messageType) {
					case MQTTMessage.CONNECT:
						readConnect(dis);
						break;
					case MQTTMessage.CONNACK:
						readConnAck(dis);
						break;
					case MQTTMessage.PUBLISH:
						readPublish(dis, qos, retain, dup);
						break;
					case MQTTMessage.PUBACK:
						readPubAck(dis);
						break;
					case MQTTMessage.PUBREC:
						readPubRec(dis);
						break;
					case MQTTMessage.PUBREL:
						readPubRel(dis, dup);
						break;
					case MQTTMessage.PUBCOMP:
						readPubComp(dis);
						break;
					case MQTTMessage.SUBSCRIBE:
						readSubscribe(dis, dup);
						break;
					case MQTTMessage.SUBACK:
						readSubAck(dis);
						break;
					case MQTTMessage.UNSUBSCRIBE:
						readUnsubscribe(dis, dup);
						break;
					case MQTTMessage.UNSUBACK:
						readUnsubAck(dis);
						break;
					case MQTTMessage.PINGREQ:
						readPingReq(dis);
						break;
					case MQTTMessage.PINGRESP:
						readPingResp(dis);
						break;
					case MQTTMessage.DISCONNECT:
						readDisconnect(dis);
						break;
					default:
						dis.close();
						throw new MQTTClientException("unknown message type: "
								+ messageType);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		});
	}

	private void readDisconnect(DataInputStream dis) {
		listener.onDisconnect();
	}

	private void readPingResp(DataInputStream dis) {
		listener.onPingResp();
	}

	private void readPingReq(DataInputStream dis) {
		listener.onPingReq();
	}

	private void readUnsubAck(DataInputStream dis) throws IOException {
		int messageId = dis.readShort();
		listener.onUnsubAck(messageId);
	}

	private void readUnsubscribe(DataInputStream dis, boolean dup)
			throws IOException {
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

	private void readSubAck(DataInputStream dis) throws IOException {
		int messageId = dis.readShort();
		byte[] qosList = null;
		if (dis.available() > 0) {
			qosList = new byte[(int) dis.available()];
			dis.readFully(qosList);
		}
		listener.onSubAck(messageId, qosList);
	}

	private void readSubscribe(DataInputStream dis, boolean dup)
			throws IOException {
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

	private void readPubComp(DataInputStream dis) throws IOException {
		int messageId = dis.readShort();
		listener.onPubComp(messageId);
	}

	private void readPubRel(DataInputStream dis, boolean dup)
			throws IOException {
		int messageId = dis.readShort();
		listener.onPubRel(messageId, dup);
	}

	private void readPubRec(DataInputStream dis) throws IOException {
		int messageId = dis.readShort();
		listener.onPubRec(messageId);
	}

	private void readPubAck(DataInputStream dis) throws IOException {
		int messageId = dis.readShort();
		listener.onPubAck(messageId);
	}

	private void readPublish(DataInputStream dis, int qos, boolean retain,
			boolean dup) throws IOException {
		String topic = dis.readUTF();
		int messageId = 0;
		if (qos > 0) {
			messageId = dis.readShort();
		}

		byte[] payload = null;
		if (dis.available() > 0) {
			payload = new byte[(int) dis.available()];
			dis.readFully(payload);
		} else {
			payload = new byte[0];
		}

		listener.onPublish(topic, messageId, payload, qos, retain, dup);
	}

	private void readConnAck(DataInputStream dis) throws IOException {
		dis.readByte();
		int responseCode = dis.readByte();
		listener.onConnAck(responseCode);
	}

	private void readConnect(DataInputStream dis) throws IOException {
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
