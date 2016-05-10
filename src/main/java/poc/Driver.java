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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class Driver {

	private static final String ADDRESS = "<redisAddr>";
	private static final String REDIS_KEY = "<addKeyHere>";
	private static final String NUMBER_OF_CHANNELS = "driver.numberOfChannels";
	private static final String NUMBER_OF_MESSAGES = "driver.numberOfMessages";
	private static final String NUMBER_OF_SUBSCRIBERS = "driver.numberOfSubscribers";
	private static final String NUMBER_OF_PUBLISHERS = "driver.numberOfPublishers";

	private static final int MAX_NUMBER_OF_PUBLISHER_THREADS = 10;

	private static Properties props;
	public static void main(String[] args) throws IOException {
		String type = System.getProperty("type", null);
		String pathToCfg = System.getProperty("cfg", null);
		if (type == null || pathToCfg == null) {
			return;
		}

		props = loadProperties(pathToCfg);

		int numberOfChannels = Integer.parseInt(props.getProperty(NUMBER_OF_CHANNELS));
		int numberOfMessages = Integer.parseInt(props.getProperty(NUMBER_OF_MESSAGES));
		int numberOfSubscribers = Integer.parseInt(props.getProperty(NUMBER_OF_SUBSCRIBERS));
		int numberOfPublishers = Integer.parseInt(props.getProperty(NUMBER_OF_PUBLISHERS));

		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(Integer.MAX_VALUE);
		poolConfig.setTestOnBorrow(true);

		JedisPool jedisPool = new JedisPool(poolConfig, ADDRESS, 6379, 1000, REDIS_KEY);

		System.out.println("-------------------------");
		System.out.println("Type: " + type);
		System.out.println("-------------------------");
		if (type.equalsIgnoreCase(AppType.SUBSCRIBER.name())) {
			for (int i = 0; i < numberOfSubscribers; i++) {
				sub("Subscriber" + i, jedisPool, numberOfChannels);
			}
		} else if (type.equalsIgnoreCase(AppType.PUBLISHER.name())) {
			for (int i = 0; i < numberOfPublishers; i++) {
				pub("Publisher" + i, jedisPool, numberOfChannels, numberOfMessages);
			}
		}
	}

	private static Properties loadProperties(String pathToCfg) throws IOException {
		InputStream is = Driver.class.getClassLoader().getResourceAsStream(pathToCfg);
		Properties props = new Properties();
		props.load(is);
		return props;
	}

	public static void sub(String subscriberName, final JedisPool jedisPool, int numberOfChannels) {
		final Subscriber subscriber = new Subscriber(subscriberName, props);
		final Jedis subscriberJedis = jedisPool.getResource();
		final ArrayList<String> channels = new ArrayList<String>();
		for (Integer channel = 1; channel <= numberOfChannels; channel++) {
			channels.add(channel.toString());
		}
		System.out.println("Subscribing to " + channels.size() + " channels");
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				System.out.println(subscriber.getName() + " received " + subscriber.getMessagesReceived().get() + " messages");
			}
		}, 0, 3000);

		ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(1);

		newFixedThreadPool.submit(new Runnable() {
			public void run() {
				subscriberJedis.subscribe(subscriber, channels.toArray(new String[channels.size()]));
			}
		});
	}

	public static void pub(final String publisherName, JedisPool jedisPool, int numberOfChannels, int numberOfMessages) {
		final List<String> messages = MessageGenerator.generateMessages(numberOfMessages);
		final AtomicLong messagesSent = new AtomicLong();

		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				System.out.println(publisherName + " sent " + messagesSent.get() + " messages");
			}
		}, 0, 3000);

		ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(MAX_NUMBER_OF_PUBLISHER_THREADS + 1);
		int threadCount;
		for (threadCount = 1; threadCount <= MAX_NUMBER_OF_PUBLISHER_THREADS; threadCount+=(numberOfChannels / MAX_NUMBER_OF_PUBLISHER_THREADS)) {
			Publisher publisher = new Publisher(publisherName, jedisPool.getResource(), props);
			Runnable runnable = new MessagePublisher(publisher, messages, messagesSent, threadCount, threadCount + (numberOfChannels / MAX_NUMBER_OF_PUBLISHER_THREADS));
			newFixedThreadPool.submit(runnable);
		}

		if (numberOfChannels % MAX_NUMBER_OF_PUBLISHER_THREADS != 0) {
			Publisher publisher = new Publisher(publisherName, jedisPool.getResource(), props);
			Runnable runnable = new MessagePublisher(publisher, messages, messagesSent, threadCount, numberOfChannels);
			newFixedThreadPool.submit(runnable);
		}
	}
}
