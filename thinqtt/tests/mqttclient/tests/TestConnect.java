package mqttclient.tests;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;

import org.junit.Test;

import thinqtt.MQTTCallback;
import thinqtt.MQTTClient;

public class TestConnect {

	@Test
	public void test() {
		MQTTCallback listener = new MQTTCallback() {

			@Override
			public void publishComplete(int messageId) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onDisconnected() {
				System.out.println("Disconnected");
			}

			@Override
			public void onConnected() {
				System.out.println("Connected");
				assertTrue(true);

			}

			@Override
			public void messageArrived(String topic, byte[] message) {
				try {
					System.out.println("Received ["
							+ new String(message, "UTF8") + "] on [" + topic
							+ "]");
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			@Override
			public void errorOccurred(Exception e) {
				e.printStackTrace();

			}

			@Override
			public void connectionLost() {
				System.out.println("Connection lost.");
			}
		};

		final MQTTClient client = new MQTTClient("localhost", 1883, null,
				listener, null);
		try {
			// Properties p = new Properties();
			// p.setProperty("keepAliveSecs", "10");
			client.connect();
			client.subscribe("testTopic", 0);
			System.in.read();
			client.disconnect();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

}
