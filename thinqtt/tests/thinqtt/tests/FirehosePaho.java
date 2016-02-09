package thinqtt.tests;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttTopic;


public class FirehosePaho {
	static MqttClient client = null;
	static Random rnd = new Random();
	static byte[] startMsg = "START".getBytes();
	static byte[] stopMsg = "STOP".getBytes();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String host = "localhost";
		int port = 1883;
		String id = Long.toHexString(rnd.nextLong());
		String topic = "firehose";
		boolean listener = false;
		
		int msgSize = 1024;
		int msgRate = 1000;
		int msgCount = 1000;
		
		for (int i=0; i<args.length; i++) {
			if ("-h".equalsIgnoreCase(args[i])) {
				host = args[i+1];
				i++;
			}
			else if ("-p".equalsIgnoreCase(args[i])) {
				port = Integer.parseInt(args[i+1]);
				i++;
			}
			else if ("-t".equalsIgnoreCase(args[i])) {
				topic = args[i+1];
				i++;
			}
			else if ("-i".equalsIgnoreCase(args[i])) {
				id = args[i+1];
				i++;
			}
			else if ("-l".equalsIgnoreCase(args[i])) {
				listener = true;
			}
			else if ("-s".equalsIgnoreCase(args[i])) {
				msgSize = Integer.parseInt(args[i+1]);
				i++;
			}
			else if ("-r".equalsIgnoreCase(args[i])) {
				msgRate = Integer.parseInt(args[i+1]);
				i++;
			}
			else if ("-c".equalsIgnoreCase(args[i])) {
				msgCount = Integer.parseInt(args[i+1]);
				i++;
			}
		}
		
		long time = -1;
		try {
			if (listener) {
				time = listen(host, port, id, topic, msgCount);
			}
			
			else {
				time = runTest(host, port, id, topic, msgSize, msgRate, msgCount);
			}
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (time > -1) {
			System.out.println("Test took " + (Long.valueOf(time).doubleValue() / 1000.0) + " seconds.");
		}

	}

	private static long listen(String host, int port, String id, final String topic, int msgCount) throws MqttException {

		final CountDownLatch latch = new CountDownLatch(1);
		final long[] start = new long[1];
		final long[] stop = new long[1];

		MqttCallback cb = new MqttCallback() {

//			@Override
//			public void connectionLost() {
//				// TODO Auto-generated method stub
//				
//			}

//			@Override
//			public void messageArrived(String topic, byte[] bs) {
//				// TODO Auto-generated method stub
////				try {
////					System.out.println(new String(bs, "UTF8"));
////				} catch (UnsupportedEncodingException e) {
////					// TODO Auto-generated catch block
////					e.printStackTrace();
////				}
//				if (Arrays.equals(bs, startMsg)) {
//					start[0] = System.currentTimeMillis();
//				}
//				else if (Arrays.equals(bs, stopMsg)) {
//					stop[0] = System.currentTimeMillis();
//					System.out.println(" done");
//					latch.countDown();
//				}
//				else {
//					System.out.print('.');
//				}
//			}

//			@Override
//			public void deliveryComplete(int messageId) {
//				// TODO Auto-generated method stub
//				
//			}
//
			@Override
			public void connectionLost(Throwable cause) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void messageArrived(MqttTopic topic, MqttMessage message)
					throws Exception {
				byte[] bs = message.getPayload();
				if (Arrays.equals(bs, startMsg)) {
					start[0] = System.currentTimeMillis();
				}
				else if (Arrays.equals(bs, stopMsg)) {
					stop[0] = System.currentTimeMillis();
					System.out.println(" done");
					latch.countDown();
				}
				else {
					System.out.print('.');
				}
			}

			@Override
			public void deliveryComplete(MqttDeliveryToken arg0) {
				// TODO Auto-generated method stub
				
			}

		};
		
		client = new MqttClient("tcp://"+host+":"+port, id);
		client.setCallback(cb);
		try {
			System.out.println("Client " + id + " listening to topic " + topic + " at " + host + ":" + port);
			client.connect();
			client.subscribe(topic);
			latch.await();
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		} 

		client.disconnect();
		return stop[0] - start[0];
	}

	private static long runTest(String host, int port, String id,
			final String topic, int msgSize, int msgRate, int msgCount) throws MqttException {

		final CountDownLatch latch = new CountDownLatch(1);

		client = new MqttClient("tcp://"+host+":"+port, id);
		
		MqttCallback cb = new MqttCallback() {

			@Override
			public void connectionLost(Throwable cause) {
				cause.printStackTrace();
				
			}

			@Override
			public void messageArrived(MqttTopic topic, MqttMessage message)
					throws Exception {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void deliveryComplete(MqttDeliveryToken token) {
				// TODO Auto-generated method stub
				
			}

		};
		
		client.setCallback(cb);
		
		long start, end;
		try {
			System.out.println("Client " + id + " sending to topic " + topic + " at " + host + ":" + port);
			System.out.println("Message rate=" + msgRate + ", size=" + msgSize + ", count=" + msgCount);
			client.connect();
//			latch.await();
			long sleep = 1000 / msgRate;
			client.getTopic(topic).publish(startMsg, 0, false);
			start = System.currentTimeMillis();
			for (int i=0; i<msgCount; i++) {
				sendMessage(client, topic, msgSize);
				Thread.sleep(sleep);
			}
			client.getTopic(topic).publish(stopMsg, 0, false);
			end = System.currentTimeMillis();
			System.out.println(" done");
			client.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		} 

		return end - start;		
	}

	private static void sendMessage(MqttClient client2, String topic,
			int msgSize) throws IOException, MqttPersistenceException, MqttException {
		byte[] msg = new byte[msgSize];
		
		rnd.nextBytes(msg);
		client.getTopic(topic).publish(msg, 0, false);
		System.out.print('.');
	}

}
