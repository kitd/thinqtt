package thinqtt;

public abstract class MQTTDecoderListener {

	protected void onDisconnect() {}

	protected void onPingResp() {}

	protected void onPingReq() {}

	protected void onUnsubAck(int messageId) {}

	protected void onUnsubscribe(int messageId, boolean dup, String[] subs) {}

	protected void onSubAck(int messageId, byte[] qosList) {}

	protected void onSubscribe(int messageId, boolean dup, String[] subs) {}

	protected void onPubComp(int messageId) {}

	protected void onPubRel(int messageId, boolean dup) {}

	protected void onPubRec(int messageId) {}

	protected void onPubAck(int messageId) {}

	protected void onPublish(String topic, int messageId, byte[] payload, int qos,
			boolean retain, boolean dup) {}

	protected void onConnAck(int responseCode) {}

	protected void onConnect(String clientId, String userName, String password,
			String protocol, int version, boolean hasLWT, String lwtMessage, 
			String lwtTopic, int lwtQos, boolean retainLWT,
			boolean cleanSession, int keepAliveSecs) {}

}
