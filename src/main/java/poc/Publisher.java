package poc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class Publisher {

	private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
	
	private final Jedis publisherJedis;

    public Publisher(Jedis publisherJedis) {
        this.publisherJedis = publisherJedis;
    }
    
    public void publishMessage(String message, String channel) {
    	long startTime = System.currentTimeMillis();
    	publisherJedis.publish(channel, message);
    	
    	long duration = System.currentTimeMillis() - startTime;
    	logger.info("Message: " + message + " published successfully in " + duration + " ms on channel " + channel);
    }
}
