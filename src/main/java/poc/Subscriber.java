package poc;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPubSub;

import java.util.Properties;

@Slf4j
public class Subscriber extends JedisPubSub {

	public static final String DELAY_PROPERTY = "subscriber.delay";

	private Properties props;
	
	public Subscriber(Properties props) {
		this.props = props;
	}

	@Override
	public void onMessage(String channel, String message) {

		long delay = Long.parseLong(props.getProperty(DELAY_PROPERTY));
		try {
			Thread.sleep(delay);
			log.info("Message received. Channel: {}, Msg: {}", channel, message);
		} catch (InterruptedException e) {
			log.error("InterruptedException " + e);
		}
	}

	@Override
	public void onPMessage(String pattern, String channel, String message) {

	}

	@Override
	public void onSubscribe(String channel, int subscribedChannels) {

	}

	@Override
	public void onUnsubscribe(String channel, int subscribedChannels) {

	}

	@Override
	public void onPUnsubscribe(String pattern, int subscribedChannels) {

	}

	@Override
	public void onPSubscribe(String pattern, int subscribedChannels) {

	}
}
