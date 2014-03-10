package thinqtt.tests;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import thinqtt.MQTTCallback;
import thinqtt.MQTTClient;

public class Firehose {
	static MQTTClient client = null;
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
		int qos = 0;
		
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
			else if ("-q".equalsIgnoreCase(args[i])) {
				qos = Integer.parseInt(args[i+1]);
				i++;
			}
		}
		
		long time;
		if (listener) {
			time = listen(host, port, id, topic, msgCount, qos);
		}
		
		else {
			time = runTest(host, port, id, topic, msgSize, msgRate, msgCount, qos);
		}
		
		if (time > -1) {
			System.out.println("Test took " + (Long.valueOf(time).doubleValue() / 1000.0) + " seconds.");
		}

	}

	private static long listen(String host, int port, String id, final String topic, int msgCount, final int qos) {

		final CountDownLatch latch = new CountDownLatch(1);
		final long[] start = new long[1];
		final long[] stop = new long[1];
		final int stopId[] = new int[1];

		client = new MQTTClient(host, port, id, null, new MQTTCallback() {

			@Override
			public void errorOccurred(Exception e) {
				e.printStackTrace();
			}

			@Override
			public void onConnected() {
				try {
					client.subscribe(topic, qos);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void onDisconnected() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void connectionLost() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void messageArrived(String topic, byte[] bs) {
				// TODO Auto-generated method stub
//				try {
//					System.out.println(new String(bs, "UTF8"));
//				} catch (UnsupportedEncodingException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
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
			public void publishComplete(int messageId) {
			}
			
		});
		
		try {
			System.out.println("Client " + id + " listening to topic " + topic + " at " + host + ":" + port);
			client.connect();
			latch.await();
			client.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		} 

		return stop[0] - start[0];
	}

	private static long runTest(String host, int port, String id,
			final String topic, int msgSize, int msgRate, int msgCount, int qos) {

		final CountDownLatch latch = new CountDownLatch(1);
		final int stopId[] = new int[1];
		
		client = new MQTTClient(host, port, id, null, new MQTTCallback() {

			@Override
			public void errorOccurred(Exception e) {
				e.printStackTrace();
			}

			@Override
			public void onConnected() {
				latch.countDown();
			}

			@Override
			public void onDisconnected() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void connectionLost() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void messageArrived(String topic, byte[] bs) {

			}

			@Override
			public void publishComplete(int messageId) {
				if (messageId == stopId[0]) {
					client.disconnect();
				}
				
			}
			
		});
		
		long start, end;
		try {
			System.out.println("Client " + id + " sending to topic " + topic + " at " + host + ":" + port);
			System.out.println("Message rate=" + msgRate + ", size=" + msgSize + ", count=" + msgCount);
			client.connect();
			latch.await();
			long sleep = 1000 / msgRate;
			client.publish(topic, startMsg, qos);
			start = System.currentTimeMillis();
			for (int i=0; i<msgCount; i++) {
				sendMessage(client, topic, msgSize, qos);
				Thread.sleep(sleep);
			}
			stopId[0] = client.publish(topic, stopMsg, qos);
			end = System.currentTimeMillis();
			if (qos == 0) {
				client.disconnect();
			}
			System.out.println(" done");
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		} 

		return end - start;		
	}

	private static void sendMessage(MQTTClient client2, String topic,
			int msgSize, int qos) throws IOException {
		byte[] msg = new byte[msgSize];
		
		rnd.nextBytes(msg);
		client.publish(topic, msg, qos);
		System.out.print('.');
	}

}
