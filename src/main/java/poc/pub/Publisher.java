package poc.pub;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class Publisher {


	public static final String MESSAGES_PER_SECOND = "publisher.messagesPerSecond";
	@Getter
	static AtomicLong messagesSent = new AtomicLong();
	static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

	@Getter
	private int messagesPerSecond;

	private JedisPool jedisPool;

	public Publisher(JedisPool jedisPool, Properties props) {
		this.messagesPerSecond = Integer.parseInt(props.getProperty(MESSAGES_PER_SECOND));
		this.jedisPool = jedisPool;
	}

	public void publishMessage(final Message message) {

		Long storeDuration = 0L;
		Long pubDuration = 0L;
		Jedis publisherJedis = null;
		try {
			publisherJedis = jedisPool.getResource();
			//store
			Long duration = System.currentTimeMillis();
			publisherJedis.setex(message.getId(), 600, message.getBody());
			storeDuration += System.currentTimeMillis() - duration;
			//publish
			duration = System.currentTimeMillis();
			publisherJedis.publish(message.getChannel(), message.getId());
			pubDuration += System.currentTimeMillis() - duration;
			//print
			Long msgSent = messagesSent.incrementAndGet();
			if (msgSent > 0 && msgSent % 500 == 0) {
				System.out.println("sent " + msgSent + " messages at " + dateFormat.format(new Date()));
				System.out.printf("  store time: %d\n", storeDuration);
				System.out.printf("  publish time: %d \n", pubDuration);
			}
		} finally {
			if (publisherJedis != null) {
				publisherJedis.close();
			}
		}
	}
}
