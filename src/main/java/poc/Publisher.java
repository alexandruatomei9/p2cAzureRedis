package poc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import java.util.Properties;

public class Publisher {

	private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
	
	private final Jedis publisherJedis;

	private Properties props;

    public Publisher(Jedis publisherJedis, Properties props) {
        this.publisherJedis = publisherJedis;
		this.props = props;
    }
    
    public void publishMessage(String message, String channel) {
    	long startTime = System.currentTimeMillis();
    	publisherJedis.publish(channel, message);
    	
    	long duration = System.currentTimeMillis() - startTime;
    	logger.info("Message: " + message + " published successfully in " + duration + " ms on channel " + channel);
    }
}
