package poc;

import lombok.extern.slf4j.Slf4j;
import poc.blob.RedisBlobStore;
import poc.pub.Message;
import poc.pub.MessageGenerator;
import poc.pub.Publisher;
import poc.sub.Subscriber;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Driver {

	private static final String ADDRESS = "p2cprototype.redis.cache.windows.net";
	private static final String REDIS_KEY = "xDipkV9siK4JDaLo08C8zjKZLg2QvQeDxEeiN0SAFUw=";

    private static final String NUMBER_OF_PUBLISHER_CHANNELS = "driver.numberOfPublisherChannels";
    private static final String NUMBER_OF_SUBSCRIBER_CHANNELS = "driver.numberOfSubscriberChannels";
    private static final String NUMBER_OF_MESSAGES = "driver.numberOfMessages";
    private static final String MAX_NUMBER_OF_PUBLISHER_THREADS = "driver.numberOfPublisherThreads";
    private static final String MAX_NUMBER_OF_SUBSCRIBER_THREADS = "driver.numberOfSubscriberThreads";

	static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

	private static Properties props;

	public static void main(String[] args) throws IOException {

        try {
            String type = System.getProperty("type", null);
            String pathToCfg = System.getProperty("cfg", null);
            if (type == null || pathToCfg == null) {
                return;
            }

            props = loadProperties(pathToCfg);
            final int numberOfMessages = Integer.parseInt(props.getProperty(NUMBER_OF_MESSAGES));

            final int numberOfPublisherChannels = Integer.parseInt(props.getProperty(NUMBER_OF_PUBLISHER_CHANNELS));
            final int maxNumberOfPublisherThreads = Integer.parseInt(props.getProperty(MAX_NUMBER_OF_PUBLISHER_THREADS));

            final int numberOfSubscriberChannels = Integer.parseInt(props.getProperty(NUMBER_OF_SUBSCRIBER_CHANNELS));
            final int maxNumberOfSubscriberThreads = Integer.parseInt(props.getProperty(MAX_NUMBER_OF_SUBSCRIBER_THREADS));

            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(1000);
            poolConfig.setTestOnBorrow(true);

            final JedisPool jedisPool = new JedisPool(poolConfig, ADDRESS, 6379, 0, REDIS_KEY);


            RedisBlobStore.init2(jedisPool);

            System.out.println("-------------------------");
            System.out.println("Type: " + type);
            System.out.println("-------------------------");
            if (type.equalsIgnoreCase(AppType.SUBSCRIBER.name())) {
                startSubscriberCounter(1000);
                sub(jedisPool, numberOfSubscriberChannels, maxNumberOfSubscriberThreads);
            } else if (type.equalsIgnoreCase(AppType.PUBLISHER.name())) {
                pub(jedisPool, numberOfPublisherChannels, numberOfMessages, maxNumberOfPublisherThreads);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
	}

    public static void sub(final JedisPool jedisPool, int numberOfChannels, int maxNumberOfSubscriberThreads) {
        System.out.println("Subscribing to " + numberOfChannels + " channels ");
        final Map<Integer, List<String>> channels = getChannelsMapping(numberOfChannels, maxNumberOfSubscriberThreads);
        //limited by the channels.size() = Math.min(numberOfChannels, maxNumberOfSubscriberThreads)
        ExecutorService newFixedThreadPool = Executors.newCachedThreadPool();
        for (final Integer subscriberId : channels.keySet()) {
            //subscribe
            newFixedThreadPool.execute(new Subscriber("Subscriber-" + subscriberId, jedisPool, channels.get(subscriberId), props));
            //new Subscriber("Subscriber-"+subscriberId, jedisPool, channels.get(subscriberId), props).subscribe();
        }
    }

    public static void pub(JedisPool jedisPool, int numberOfChannels, int numberOfMessages, int maxNumberOfPublisherThreads) {
        final List<Message> messages = MessageGenerator.generateMessages(numberOfMessages, numberOfChannels);
        ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(maxNumberOfPublisherThreads);
        final Publisher publisher = new Publisher(jedisPool, props);
        Long duration = System.currentTimeMillis();
        for (final Message m : messages) {
            newFixedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    publisher.publishMessage(m);
                }
            });
        }
        newFixedThreadPool.shutdown();
        try {
            newFixedThreadPool.awaitTermination(10, TimeUnit.MINUTES);
            duration = System.currentTimeMillis() - duration;
        } catch (InterruptedException e) {
            //ignore
        }
        if (newFixedThreadPool.isTerminated()) {
            System.out.printf("Publisher sent %d messages, in %d seconds, averaging a %d ms/message ", Publisher.getMessagesSent().get(), duration / 1000, duration / Publisher.getMessagesSent().get());
        }
    }

    private static Properties loadProperties(String pathToCfg) throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(pathToCfg));
        return props;
    }

    private static Map<Integer, List<String>> getChannelsMapping(int numberOfChannels, int maxNumberOfSubscriberThreads) {
        int numberOfSubscribers = Math.min(numberOfChannels, maxNumberOfSubscriberThreads);
        Map<Integer, List<String>> channels = new HashMap<>();
        for (int i = 0; i < numberOfSubscribers; i++) {
            channels.put(i, new LinkedList<String>());
        }
        for (Integer channel = 0; channel < numberOfChannels; ) {
            for (int subscriber = 0; subscriber < numberOfSubscribers; subscriber++, channel++) {
                channels.get(subscriber).add(channel.toString());
            }
        }
        return channels;
    }

    private static void startSubscriberCounter(int displayDelay) {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            int numberOfTimesSameMessagehasBeenDisplayed = 4; // > 3
            public void run() {
                Long messagesReceived = Subscriber.getMessagesReceived().get();
                Long messagesRead = Subscriber.getMessagesRead().get();
                numberOfTimesSameMessagehasBeenDisplayed = !messagesRead.equals(messagesReceived) ? 0 : ++numberOfTimesSameMessagehasBeenDisplayed;
                if (numberOfTimesSameMessagehasBeenDisplayed < 3) {
                    System.out.printf(" received %d messages, and finished reading from store %d at %s\n",
                            messagesReceived, Subscriber.getMessagesRead().get(), dateFormat.format(new Date()));
                }
            }
        }, 0, displayDelay);
    }

}
