package io.grisu.usvcs.rabbitmq;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.rabbitmq.client.*;
import io.grisu.core.exceptions.GrisuException;
import io.grisu.pojo.AbstractPojo;
import io.grisu.pojo.utils.JSONUtils;
import io.grisu.usvcs.Client;

public class ClientRabbitMQ implements Client {
    private final Channel channel;
    private final String replyQueueName;
    private final Map<String, Object[]> listeners;
    private final AtomicBoolean running;
    private final Consumer consumer;

    private String consumerTag;

    public ClientRabbitMQ(Channel channel) throws IOException {
        this.channel = channel;
        this.replyQueueName = channel.queueDeclare("reply_" + java.util.UUID.randomUUID().toString(),
            true, true, true, null).getQueue();
        this.listeners = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(false);
        this.consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                Object[] entry = listeners.remove(properties.getCorrelationId());
                if (entry != null) {
                    CompletableFuture listener = (CompletableFuture<AbstractPojo>) entry[1];

                    final Object[] objects = RPCUtils.decodeMessage(body);

                    if (RabbitMQConstants.KO.equals(objects[0])) {
                        listener.completeExceptionally(GrisuException.build(JSONUtils.decode((byte[]) objects[1], Map.class)));
                    } else {

                        try {
                            final Object ret = JSONUtils.decode((byte[]) objects[1], ((ParameterizedType) entry[0]).getActualTypeArguments()[0]);
                            listener.complete(ret);
                        } catch (RuntimeException e) {
                            listener.completeExceptionally(e);
                        }
                    }
                }
            }
        };
    }

    public void start() {
        if (running.get()) {
            return;
        }
        running.set(true);

        try {
            consumerTag = channel.basicConsume(replyQueueName, true, consumer);
        } catch (Exception e) {
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
