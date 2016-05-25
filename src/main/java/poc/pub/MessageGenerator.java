package poc.pub;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

public class MessageGenerator {

	public final static String message_2KB = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum." +
			"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum." +
			"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

	public static List<Message> generateMessages(int numberOfMessages, int numberOfChannels) {
		List<Message> messages = new ArrayList<>(numberOfMessages);
		Integer channel = 0;
		Set<Integer> channels = new TreeSet<>();
		for (int i = 0; i < numberOfMessages; i++) {
			String messageId = UUID.randomUUID().toString();
			messages.add(new Message(messageId, channel.toString(), getBody(messageId)));
			channel = ++channel < numberOfChannels ? channel : 0;
			channels.add(channel);
		}
		System.out.printf("Generated %d messages for channels %s \n", numberOfMessages, channels);
		return messages;
	}

	private static String getBody(String messageId) {
		return message_2KB + messageId;
	}
}
