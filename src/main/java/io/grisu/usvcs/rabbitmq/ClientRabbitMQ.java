package io.grisu.usvcs.rabbitmq;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import io.grisu.pojo.utils.JSONUtils;
import io.grisu.usvcs.Client;
import io.grisu.usvcs.rabbitmq.consumers.ClientDefault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientRabbitMQ implements Client {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Channel channel;
    private final String replyQueueName;
    private final Map<String, Object[]> listeners;
    private final AtomicBoolean running;
    private final ClientDefault clientDefault;

    private String consumerTag;

    public ClientRabbitMQ(Channel channel) throws IOException {
        this.channel = channel;
        this.replyQueueName = channel.queueDeclare().getQueue();
        this.listeners = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(false);
        clientDefault = new ClientDefault(listeners, channel);
    }

    public void start() {
        if (running.get()) {
            return;
        }
        running.set(true);

        try {
            consumerTag = channel.basicConsume(replyQueueName, true, clientDefault);
            log.debug("consumerTag: " + consumerTag);
        } catch (Exception e) {
            log.error("exception", e);
        }
    }

    public void stop() {
        if (consumerTag != null) {
            try {
                channel.basicCancel(consumerTag);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public <R> CompletableFuture<R> send(String uServiceQueue, String nServiceIdentifier, Type returnType, Object... params) {
        String correlationId = java.util.UUID.randomUUID().toString();

        final AMQP.BasicProperties props = new AMQP.BasicProperties
            .Builder()
            .correlationId(correlationId)
            .replyTo(replyQueueName)
            .build();

        CompletableFuture future = new CompletableFuture<>();
        listeners.put(correlationId, new Object[] { returnType, future });

        try {
            channel.basicPublish("", uServiceQueue, props, RPCUtils.encodeMessage(nServiceIdentifier, JSONUtils.encode(params)));
        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

}
