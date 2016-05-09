package poc;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Driver {

	private static final String ADDRESS = "<redisAddr>";
	private static final String REDIS_KEY = "<addKeyHere>";
	private static final String NUMBER_OF_CHANNELS = "driver.numberOfChannels";
	private static final String NUMBER_OF_SUBSCRIBERS = "driver.numberOfSubscribers";
	private static final String NUMBER_OF_PUBLISHERS = "driver.numberOfPublishers";

	private static Properties props;
	public static void main(String[] args) throws IOException {
		String type = System.getProperty("type", null);
		String pathToCfg = System.getProperty("cfg", null);
		if (type == null || pathToCfg == null) {
			return;
		}

		props = loadProperties(pathToCfg);

		int numberOfChannels = Integer.parseInt(props.getProperty(NUMBER_OF_CHANNELS));

		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(5000);
		poolConfig.setTestOnBorrow(true);

		JedisPool jedisPool = new JedisPool(poolConfig, ADDRESS, 6379, 1000, REDIS_KEY);

		System.out.println("-------------------------");
		System.out.println("Type: " + type);
		System.out.println("-------------------------");
		if (type.equalsIgnoreCase(AppType.SUBSCRIBER.name())) {
			sub(jedisPool, numberOfChannels);
		} else if (type.equalsIgnoreCase(AppType.PUBLISHER.name())) {
			pub(jedisPool, numberOfChannels);
		}
	}

	private static Properties loadProperties(String pathToCfg) throws IOException {
		InputStream is = Driver.class.getClassLoader().getResourceAsStream(pathToCfg);
		Properties props = new Properties();
		props.load(is);
		return props;
	}

	public static void sub(final JedisPool jedisPool, int numberOfChannels) {
		final Subscriber subscriber = new Subscriber("Subscriber", props);
		Jedis subscriberJedis = jedisPool.getResource();
		ArrayList<String> channels = new ArrayList<String>();
		for (Integer channel = 1; channel <= numberOfChannels; channel++) {
			channels.add(channel.toString());
		}
		log.info("Subscribing to " + channels.size() + " channels");
		subscriberJedis.subscribe(subscriber, channels.toArray(new String[channels.size()]));

		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				System.out.println(subscriber.getName() + " received " + subscriber.getMessagesReceived().get() + " messages");
			}
		}, 0, 5000);
	}

	public static void pub(final JedisPool jedisPool, final int numberOfChannels) {
		final Publisher publisher = new Publisher(jedisPool.getResource(), props);
		final List<String> messages = MessageGenerator.generateMessages(publisher.getNumberOfMessages());

		ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(numberOfChannels * 2);
		for (Integer channel = 1; channel < numberOfChannels; channel++) {
			final String CHANNEL_NAME = channel.toString();
			final List<String> messagesCpy = new ArrayList<String>(messages);
			newFixedThreadPool.submit(new Runnable() {
				public void run() {
					for (String message : messagesCpy) {
						publisher.publishMessage(message, CHANNEL_NAME);
					}
				}
			});
		}
	}
}
