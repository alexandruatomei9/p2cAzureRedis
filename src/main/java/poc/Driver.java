package poc;

import lombok.extern.slf4j.Slf4j;
import poc.blob.RedisBlobStore;
import poc.pub.MessageGenerator;
import poc.pub.MessagePublisher;
import poc.pub.Publisher;
import poc.sub.ChannelSubscriber;
import poc.sub.Subscriber;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class Driver {

	private static final String ADDRESS = "p2cprototype.redis.cache.windows.net";
	private static final String REDIS_KEY = "xDipkV9siK4JDaLo08C8zjKZLg2QvQeDxEeiN0SAFUw=";
	private static final String NUMBER_OF_CHANNELS = "driver.numberOfChannels";
	private static final String NUMBER_OF_MESSAGES = "driver.numberOfMessages";
	private static final String NUMBER_OF_SUBSCRIBERS = "driver.numberOfSubscribers";
	private static final String NUMBER_OF_PUBLISHERS = "driver.numberOfPublishers";

	static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

	private static final int MAX_NUMBER_OF_PUBLISHER_THREADS = 50;
	private static final int MAX_NUMBER_OF_SUBSCRIBER_THREADS = 50;

	private static Properties props;
	public static void main(String[] args) throws IOException {

        try {
            String type = System.getProperty("type", null);
            String pathToCfg = System.getProperty("cfg", null);
            if (type == null || pathToCfg == null) {
                return;
            }

            props = loadProperties(pathToCfg);

            final int numberOfChannels = Integer.parseInt(props.getProperty(NUMBER_OF_CHANNELS));
            final int numberOfMessages = Integer.parseInt(props.getProperty(NUMBER_OF_MESSAGES));
            int numberOfSubscribers = Integer.parseInt(props.getProperty(NUMBER_OF_SUBSCRIBERS));
            int numberOfPublishers = Integer.parseInt(props.getProperty(NUMBER_OF_PUBLISHERS));

            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(Integer.MAX_VALUE);
            poolConfig.setTestOnBorrow(true);

            final JedisPool jedisPool = new JedisPool(poolConfig, ADDRESS, 6379, 0, REDIS_KEY);

            RedisBlobStore.init(ADDRESS, REDIS_KEY);

            System.out.println("-------------------------");
            System.out.println("Type: " + type);
            System.out.println("-------------------------");
            if (type.equalsIgnoreCase(AppType.SUBSCRIBER.name())) {
                for (int i = 0; i < numberOfSubscribers; i++) {
                    ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(1);
                    final String subscriberName = "Subscriber" + i;
                    newFixedThreadPool.submit(new Runnable() {
                        public void run() {
                            sub(subscriberName, jedisPool, numberOfChannels);
                        }
                    });
                }
            } else if (type.equalsIgnoreCase(AppType.PUBLISHER.name())) {
                for (int i = 0; i < numberOfPublishers; i++) {
                    ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(1);
                    final String publisherName = "Publisher" + i;
                    newFixedThreadPool.submit(new Runnable() {
                        public void run() {
                            pub(publisherName, jedisPool, numberOfChannels, numberOfMessages);
                        }
                    });
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
	}

	private static Properties loadProperties(String pathToCfg) throws IOException {
		InputStream is = Driver.class.getClassLoader().getResourceAsStream(pathToCfg);
		Properties props = new Properties();
		props.load(is);
		return props;
	}

	public static void sub(final String subscriberName, final JedisPool jedisPool, int numberOfChannels) {
		final ArrayList<String> channels = new ArrayList<String>();
		for (Integer channel = 1; channel <= numberOfChannels; channel++) {
			channels.add(channel.toString());
		}
		System.out.println("Subscribing to " + channels.size() + " channels ");
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
            long lastValue = 0;
			public void run() {
				Date date = new Date();
                if (Subscriber.getMessagesReceived().get() > lastValue) {
                    lastValue = Subscriber.getMessagesReceived().get();
                    System.out.println(subscriberName + " received " + lastValue +" messages at " + dateFormat.format(date));
                }
			}
		}, 0, 1000);

		ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(MAX_NUMBER_OF_SUBSCRIBER_THREADS + 1);
		int maxChannel = 0;
		int minChannel = 0;
		if (numberOfChannels > MAX_NUMBER_OF_SUBSCRIBER_THREADS) {
			for (int threadCount = 1; threadCount <= MAX_NUMBER_OF_SUBSCRIBER_THREADS; threadCount++) {
				Subscriber subscriber = new Subscriber(subscriberName, props);
				Jedis subscriberJedis = jedisPool.getResource();
				maxChannel = maxChannel + (numberOfChannels / MAX_NUMBER_OF_SUBSCRIBER_THREADS);
				List<String> channelsToSubscribe = channels.subList(minChannel, maxChannel);
				Runnable runnable = new ChannelSubscriber(subscriberJedis, subscriber, channelsToSubscribe);
				minChannel = maxChannel;
				newFixedThreadPool.submit(runnable);
			}
		}

		if (numberOfChannels % MAX_NUMBER_OF_SUBSCRIBER_THREADS != 0) {
			Subscriber subscriber = new Subscriber(subscriberName, props);
			Jedis subscriberJedis = jedisPool.getResource();
			List<String> channelsToSubscribe = channels.subList(minChannel, channels.size());
			Runnable runnable = new ChannelSubscriber(subscriberJedis, subscriber, channelsToSubscribe);
			newFixedThreadPool.submit(runnable);
		}
	}

	public static void pub(final String publisherName, JedisPool jedisPool, int numberOfChannels, int numberOfMessages) {
		final List<String> messages = MessageGenerator.generateMessages(numberOfMessages);
		final AtomicLong messagesSent = new AtomicLong();

		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
            long lastValue = 0;
            public void run() {
				Date date = new Date();
                if (messagesSent.get() > lastValue) {
                    lastValue = messagesSent.get();
                    System.out.println(publisherName + " sent " + lastValue + " messages at " + dateFormat.format(date));
                }
			}
		}, 0, 1000);

		ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(MAX_NUMBER_OF_PUBLISHER_THREADS + 1);
		int maxChannel = 1;
		int minChannel = 1;
		if (numberOfChannels > MAX_NUMBER_OF_PUBLISHER_THREADS) {
			for (int threadCount = 1; threadCount <= MAX_NUMBER_OF_PUBLISHER_THREADS; threadCount++) {
				maxChannel = maxChannel + (numberOfChannels / MAX_NUMBER_OF_PUBLISHER_THREADS);
				Publisher publisher = new Publisher(publisherName, jedisPool.getResource(), props);
				Runnable runnable = new MessagePublisher(publisher, messages, messagesSent, minChannel, maxChannel);
				minChannel = maxChannel;
				newFixedThreadPool.submit(runnable);
			}
		}

		if (numberOfChannels % MAX_NUMBER_OF_PUBLISHER_THREADS != 0) {
			Publisher publisher = new Publisher(publisherName, jedisPool.getResource(), props);
			Runnable runnable = new MessagePublisher(publisher, messages, messagesSent, minChannel, numberOfChannels + 1);
			newFixedThreadPool.submit(runnable);
		}
	}
}
