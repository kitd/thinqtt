package thinqtt;

import java.util.HashMap;
import java.util.Map;

public abstract class MQTTMessageStore {
	
	public abstract void put(int type, int id, int qos, String topic, byte[] msg, boolean retained);

	public abstract MQTTMessage get(int id);

	public abstract MQTTMessage delete(int id);
	
	public abstract boolean contains(int id);
	
	public abstract int count();

	public static class InMemory extends MQTTMessageStore {
		private final Map<Integer, MQTTMessage> store = new HashMap<>();

		public void put(int type, int id, int qos, String topic, byte[] msg, boolean retained) {
			store.put(id, new MQTTMessage(type, id, qos, topic, msg, retained));
		}

		public MQTTMessage get(int id) {
			return store.get(id);
		}

		public MQTTMessage delete(int id) {
			return store.remove(id);
		}
		
		public boolean contains(int id) {
			return store.containsKey(id);
		}
		
		public int count() {
			return store.size();
		}

	}
}
