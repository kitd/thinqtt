package thinqtt;

public interface MQTTCallback {

	public void errorOccurred(Exception e);
	public void onConnected();
	public void onDisconnected();
	public void connectionLost();
	public void messageArrived(String topic, byte[] bs);
	public void publishComplete(int messageId);
}
