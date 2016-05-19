package poc.sub;

import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ChannelSubscriber implements Runnable {
    private Jedis subscriberJedis;
    private Subscriber subscriber;
    private List<String> channels;

    public ChannelSubscriber(Jedis subscriberJedis, Subscriber subscriber, List<String> channels) {
        this.subscriberJedis = subscriberJedis;
        this.subscriber = subscriber;
        this.channels = channels;
    }

    public void run() {
        subscriberJedis.subscribe(subscriber, channels.toArray(new String[channels.size()]));
    }

}