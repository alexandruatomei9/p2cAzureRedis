package poc.sub;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import poc.blob.RedisBlobStore;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class Subscriber extends JedisPubSub implements Runnable {

	public static final String DELAY_PROPERTY = "subscriber.delay";
    @Getter
    static AtomicLong messagesReceived = new AtomicLong();
    @Getter
    static AtomicLong messagesRead = new AtomicLong();
    private Long delay;
    @Getter
	private String name;

    private JedisPool jedisPool;

    private List<String> channels;

    private ExecutorService workerExecutor = Executors.newFixedThreadPool(10);

    public Subscriber(String name, JedisPool jedisPool, List<String> channels, Properties properties) {
        this.name = name;
        this.channels = channels;
        this.jedisPool = jedisPool;
        this.delay = Long.valueOf(properties.getProperty(DELAY_PROPERTY));
    }

    @Override
    public void onMessage(String channel, String messageId) {
        messagesReceived.incrementAndGet();
        try {
            workerExecutor.execute(new Worker(delay, messageId));
        } catch (Exception e) {
            log.error(e.getMessage());
        }
	}

    @Override
    public void run() {
        Thread.currentThread().setName(getName());
        try (Jedis subscriberJedis = jedisPool.getResource()) {
            System.out.println(getName() + " subscribing to channels: " + channels);
            subscriberJedis.subscribe(this, channels.toArray(new String[]{}));
        }
    }

    /**
     */
    private class Worker implements Runnable {

        private long delay;
        private String messageId;

        public Worker(Long delay, String messageId) {
            this.delay = delay;
            this.messageId = messageId;
        }

        public void run() {
            try {
                if (delay > 0) {
                    Thread.sleep(delay);
                }
                String msg = RedisBlobStore.get(messageId);
                Objects.requireNonNull(msg);
                messagesRead.incrementAndGet();
            } catch (InterruptedException e) {
                log.error("InterruptedException " + e.getMessage());
            }
        }
    }
}
