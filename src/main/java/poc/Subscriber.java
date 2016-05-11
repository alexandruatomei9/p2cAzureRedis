package poc;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import redis.clients.jedis.JedisPubSub;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class Subscriber extends JedisPubSub {

	public static final String DELAY_PROPERTY = "subscriber.delay";

	private Properties props;

	@Getter
	private String name;

	@Getter
	@Setter
	AtomicLong messagesReceived = new AtomicLong();

	public Subscriber(String name, Properties props) {
		this.name = name;
		this.props = props;
	}

	@Override
	public void onMessage(String channel, String message) {

		long delay = Long.parseLong(props.getProperty(DELAY_PROPERTY));
		try {
			Thread.sleep(delay);
			//log.info("Message received. Channel: {}, Msg: {}", channel, message);
			messagesReceived.incrementAndGet();
		} catch (InterruptedException e) {
			log.error("InterruptedException " + e);
		}
	}
}
