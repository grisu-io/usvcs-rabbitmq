package io.grisu.usvcs.rabbitmq.consumers;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.grisu.core.exceptions.GrisuException;
import io.grisu.pojo.AbstractPojo;
import io.grisu.pojo.utils.JSONUtils;
import io.grisu.usvcs.rabbitmq.RPCUtils;
import io.grisu.usvcs.rabbitmq.RabbitMQConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientDefault extends DefaultConsumer {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Map<String, Object[]> listeners;

    public ClientDefault(Map<String, Object[]> listeners, Channel channel) {
        super(channel);
        this.listeners = listeners;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws
        IOException {
        log.debug("received message " + new String(body));

        Object[] entry = listeners.remove(properties.getCorrelationId());
        if (entry != null) {
            CompletableFuture listener = (CompletableFuture<AbstractPojo>) entry[1];

            final Object[] objects = RPCUtils.decodeMessage(body);

            if (RabbitMQConstants.KO.equals(objects[0])) {
                listener.completeExceptionally(GrisuException.build(JSONUtils.decode((byte[]) objects[1], Map.class)));
            } else {
                final Object ret = JSONUtils.decode((byte[]) objects[1], ((ParameterizedType) entry[0]).getActualTypeArguments()[0]);
                listener.complete(ret);
            }
        }
    }

}
