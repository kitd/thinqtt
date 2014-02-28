package thinqtt;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class MQTTEncoder {
	private final DataOutputStream dos;

	public MQTTEncoder(OutputStream os) {
		this.dos = new DataOutputStream(os);
	}

	public void writePing() throws IOException {
		writeFixedHeader(dos, MQTTMessage.PINGREQ, false, 0, false);
		writeRemainingLength(dos, 0);
		dos.flush();
	}

	public void writeConnect(String clientId, String user, String password,
			String lwtTopic, String lwtMsg, int lwtQos, boolean lwtRetain,
			boolean cleanSession, int keepAliveSecs) throws IOException {

		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		DataOutputStream dos2 = new DataOutputStream(payload);
		dos2.writeUTF("MQIsdp");
		dos2.writeByte(3);
		dos2.writeByte((user != null ? 0x80 : 0x00)
				| (password != null ? 0x40 : 0x00) 
				| (lwtRetain ? 0x20 : 0x00)
				| (lwtQos << 3) 
				| (lwtTopic != null ? 0x04 : 0x00)
				| (cleanSession ? 0x02 : 0x00));
		dos2.writeShort(keepAliveSecs);
		dos2.writeUTF(clientId);
		if (lwtTopic != null) {
			dos2.writeUTF(lwtTopic);
			dos2.writeUTF(lwtMsg);
		}
		if (user != null) {
			dos2.writeUTF(user);
		}
		if (password != null) {
			dos2.writeUTF(password);
		}

		writeFixedHeader(dos, MQTTMessage.CONNECT, false, 1, false);
		writeRemainingLength(dos, payload.size());
		payload.writeTo(dos);
		dos.flush();
	}

	private void writeFixedHeader(DataOutputStream dos, int messageType,
			boolean dup, int qos, boolean retain) throws IOException {
		int fixedHeader = (messageType << 4) | (dup ? 0x08 : 0x00) | (qos << 1)
				| (retain ? 0x01 : 0x00); 
		dos.writeByte(fixedHeader);
	}

	private void writeRemainingLength(DataOutputStream dos, int len)
			throws IOException {
		// ALGORITHM FOR ENCODING REMAINING LENGTH
		// do
		// 	digit = X MOD 128
		// 	X = X DIV 128
		// 	// if there are more digits to encode, set the top bit of this digit
		// 	if ( X > 0 )
		// 		digit = digit OR 0x80
		// 	endif
		// 	'output' digit
		// while ( X> 0 )
		int x = len;
		do {
			int digit = x % 128;
			x /= 128;
			if (x > 0) {
				digit |= 0x0080;
			}
			dos.write(digit);
		} while (x > 0);
	}

	public void writeDisconnect() throws IOException {
		writeFixedHeader(dos, MQTTMessage.DISCONNECT, false, 0, false);
		writeRemainingLength(dos, 0);
		dos.flush();
	}

	public void writeSubscribe(int msgId, String topicPattern, int qos)
			throws IOException {
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		DataOutputStream dos2 = new DataOutputStream(payload);
		dos2.writeShort(msgId);
		dos2.writeUTF(topicPattern);
		dos2.writeByte(qos);

		writeFixedHeader(dos, MQTTMessage.SUBSCRIBE, false, 1, false);
		writeRemainingLength(dos, payload.size());
		payload.writeTo(dos);
		dos.flush();
	}

	public void writePubAck(int messageId) throws IOException {
		writeFixedHeader(dos, MQTTMessage.PUBACK, false, 0, false);
		writeRemainingLength(dos, 2);
		dos.writeShort(messageId);
		dos.flush();
	}

	public void writePubRec(int messageId) throws IOException {
		writeFixedHeader(dos, MQTTMessage.PUBREC, false, 0, false);
		writeRemainingLength(dos, 2);
		dos.writeShort(messageId);
		dos.flush();
	}

	public void writePubRel(int messageId) throws IOException {
		writeFixedHeader(dos, MQTTMessage.PUBREL, false, 1, false);
		writeRemainingLength(dos, 2);
		dos.writeShort(messageId);
		dos.flush();
	}

	public void writePubComp(int messageId) throws IOException {
		writeFixedHeader(dos, MQTTMessage.PUBCOMP, false, 0, false);
		writeRemainingLength(dos, 2);
		dos.writeShort(messageId);
		dos.flush();
	}

	public void writePublish(String topic, byte[] message, int msgId, int qos)
			throws IOException {
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		DataOutputStream dos2 = new DataOutputStream(payload);
		dos2.writeUTF(topic);
		if (qos > 0) {
			dos2.writeShort(msgId);
		}
		dos2.write(message);

		writeFixedHeader(dos, MQTTMessage.PUBLISH, false, qos, false);
		writeRemainingLength(dos, payload.size());
		payload.writeTo(dos);
		dos.flush();
	}
}
