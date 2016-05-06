package poc;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

@Slf4j
public class Driver {


	private static String ADDRESS = "<redisAddr>";
	private static String REDIS_KEY = "<addKeyHere>";

	private static Properties props;
	public static void main(String[] args) throws IOException {
		String type = System.getProperty("type", null);
		String pathToCfg = System.getProperty("cfg", null);
		if (type == null || pathToCfg == null) {
			return;
		}

		props = loadProperties(pathToCfg);

		String msgPerSec = System.getProperty("msgPerSec", "5");
		int messagesPerSecond = Integer.parseInt(msgPerSec);

		String delayTime = System.getProperty("delay", "100");
		long delay = Long.parseLong(delayTime);

		String channelRange = System.getProperty("channelRange", "1-500");
		int minChannel = Integer.parseInt(channelRange.split("-")[0]);
		int maxChannel = Integer.parseInt(channelRange.split("-")[1]);

		if (minChannel >= maxChannel) {
			System.out.println("MaxChannel should be greater than MinChannel");
			return;
		}

		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(5000);
		poolConfig.setTestOnBorrow(true);

		JedisPool jedisPool = new JedisPool(poolConfig, ADDRESS, 6379, 1000, REDIS_KEY);

		System.out.println("-------------------------");
		System.out.println("Type: " + type);
		System.out.println("-------------------------");
		if (type.equalsIgnoreCase(AppType.SUBSCRIBER.name())) {
			sub(jedisPool, minChannel, maxChannel, delay);
		} else if (type.equalsIgnoreCase(AppType.PUBLISHER.name())) {
			pub(jedisPool, minChannel, maxChannel, messagesPerSecond);
		}
	}

	private static Properties loadProperties(String pathToCfg) throws IOException {
		InputStream is = Driver.class.getResourceAsStream(pathToCfg);
		Properties props = new Properties();
		props.load(is);
		return props;
	}

	public static void sub(final JedisPool jedisPool, final int minChannel, final int maxChannel, long delay) {
		Subscriber subscriber = new Subscriber(props);
		Jedis subscriberJedis = jedisPool.getResource();
		ArrayList<String> channels = new ArrayList<String>();
		for (Integer channel = minChannel; channel <= maxChannel; channel++) {
			channels.add(channel.toString());
		}
		log.info("Subscribing to " + channels.size() + " channels");
		subscriberJedis.subscribe(subscriber, channels.toArray(new String[channels.size()]));
	}

	public static void pub(final JedisPool jedisPool, final int minChannel, final int maxChannel, final int messagesPerSecond) {
		final List<String> messages = MessageGenerator.generateMessages(messagesPerSecond);
		final Publisher publisher = new Publisher(jedisPool.getResource(), props);
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				for (Integer channel = minChannel; channel < maxChannel; channel++) {
					String CHANNEL_NAME = channel.toString();
					for (String message : messages) {
						publisher.publishMessage(message, CHANNEL_NAME);
					}
				}
			}
		}, 1000, 1000);
	}
}
