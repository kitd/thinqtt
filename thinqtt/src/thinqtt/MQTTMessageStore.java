package thinqtt;

import java.util.HashMap;
import java.util.Map;

public class MQTTMessageStore {
	private final Map<Integer, MQTTMessage> store = new HashMap<>();
	
	public void put(int type, int id, int qos, String topic, byte[] msg) {
		store.put(id, new MQTTMessage(type, id, qos, topic, msg));
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
