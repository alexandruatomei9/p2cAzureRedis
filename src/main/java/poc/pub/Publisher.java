package poc.pub;

import lombok.Getter;

import lombok.extern.slf4j.Slf4j;
import poc.blob.RedisBlobStore;
import poc.pub.MessageGenerator;
import redis.clients.jedis.Jedis;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class Publisher {

	public static final String MESSAGES_PER_SECOND = "publisher.messagesPerSecond";
	
	private final Jedis publisherJedis;

	@Getter
	private String name;

	@Getter
	private int messagesPerSecond;

    public Publisher(String name, Jedis publisherJedis, Properties props) {
		this.name = name;
        this.publisherJedis = publisherJedis;
		this.messagesPerSecond = Integer.parseInt(props.getProperty(MESSAGES_PER_SECOND));
    }
    
    public void publishMessage(String message, String channel, AtomicLong messagesSent) {
        RedisBlobStore.put(message, MessageGenerator.message_2KB + message);
        publisherJedis.publish(channel, message);
		messagesSent.incrementAndGet();
    	//log.info("Message: " + message + " published successfully on channel " + channel);
    }
}
