package poc;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPubSub;

@Slf4j
public class Subscriber extends JedisPubSub {

	private long delay;
	
	public Subscriber(long delay) {
		this.delay = delay;
	}

	@Override
	public void onMessage(String channel, String message) {
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
