package poc;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class MessageGenerator {

	public static List<String> generateMessages(int numberOfMessages) {
		List<String> messages = new ArrayList<String>(numberOfMessages);
		
		for (int i=0; i < numberOfMessages; i++) {
			messages.add(UUID.randomUUID().toString());
		}
		
		return messages;
	}
}
