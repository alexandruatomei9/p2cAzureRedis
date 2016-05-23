package poc.pub;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MessagePublisher implements Runnable {
    private final List<String> messages;
    private final Publisher publisher;
    private final Integer minChannel;
    private final Integer maxChannel;
    private AtomicLong messagesSent;

    private static Long SECOND = 1000l;

    public MessagePublisher(Publisher publisher, List<String> messages, AtomicLong messagesSent, int minChannel, int maxChannel) {
        this.publisher = publisher;
        this.messages = messages;
        this.messagesSent = messagesSent;
        this.minChannel = minChannel;
        this.maxChannel = maxChannel;
    }

    public void run() {
        for (String message : messages) {
            for (Integer channel = minChannel; channel < maxChannel; channel++) {
                publisher.publishMessage(message, channel.toString(), messagesSent);
            }
        }
    }
}