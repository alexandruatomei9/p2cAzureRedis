package poc;

import lombok.Getter;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.Properties;

@Slf4j
public class Publisher {

	public static final String NUMBER_OF_MESSAGES = "publisher.numberOfMessages";
	public static final String MESSAGES_PER_SECOND = "publisher.messagesPerSecond";
	
	private final Jedis publisherJedis;

	@Getter
	private int messagesPerSecond;

	@Getter
	private int numberOfMessages;

    public Publisher(Jedis publisherJedis, Properties props) {
        this.publisherJedis = publisherJedis;
		this.messagesPerSecond = Integer.parseInt(props.getProperty(MESSAGES_PER_SECOND));
		this.numberOfMessages = Integer.parseInt(props.getProperty(NUMBER_OF_MESSAGES));
    }
    
    public void publishMessage(String message, String channel) {
    	long startTime = System.currentTimeMillis();
    	publisherJedis.publish(channel, message);
    	long duration = System.currentTimeMillis() - startTime;
    	log.info("Message: " + message + " published successfully in " + duration + " ms on channel " + channel);
    }
}
