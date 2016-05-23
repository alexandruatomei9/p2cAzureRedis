package poc.sub;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import poc.blob.RedisBlobStore;
import redis.clients.jedis.JedisPubSub;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class Subscriber extends JedisPubSub {

	public static final String DELAY_PROPERTY = "subscriber.delay";

    private Long delay;

    ExecutorService executorService = Executors.newFixedThreadPool(1000);

    @Getter
    static AtomicLong messagesReceived = new AtomicLong();

    @Getter
	private String name;

	public Subscriber(String name, Properties props) {
		this.name = name;
        delay = Long.parseLong(props.getProperty(DELAY_PROPERTY));
	}

	@Override
	public void onMessage(String channel, String message) {

		try {
			//log.info("Message received. Channel: {}, Msg: {}", channel, message);
            executorService.submit(new Worker(delay, message));
		} catch (Exception e) {
            log.error(e.getMessage());
        }
	}

    /**
     */
    @Slf4j
    public static class Worker implements Runnable {

        private long delay;
        private String messageId;

        public Worker(Long delay, String messageId) {
            this.delay = delay;
            this.messageId = messageId;
        }

        public void run() {
            try {
                Thread.sleep(delay);
                String msg = RedisBlobStore.get(messageId);
                Objects.requireNonNull(msg);
                messagesReceived.incrementAndGet();
            } catch (InterruptedException e) {
                Worker.log.error("InterruptedException " + e.getMessage());
            }
        }
    }
}
