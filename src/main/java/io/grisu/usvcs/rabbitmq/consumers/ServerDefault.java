package io.grisu.usvcs.rabbitmq.consumers;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.grisu.core.GrisuConstants;
import io.grisu.core.exceptions.GrisuException;
import io.grisu.core.utils.MapBuilder;
import io.grisu.pojo.utils.JSONUtils;
import io.grisu.usvcs.rabbitmq.RPCUtils;
import io.grisu.usvcs.rabbitmq.RabbitMQConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerDefault extends DefaultConsumer {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private Channel channel;
    private final String rpcQueueName;
    private final Map<String, Method> nServicesHandlers;
    private final Object uServiceImpl;

    public ServerDefault(Channel channel, String rpcQueueName, Map<String, Method> nServicesHandlers, Object uServiceImpl) {
        super(channel);
        this.channel = channel;
        this.rpcQueueName = rpcQueueName;
        this.nServicesHandlers = nServicesHandlers;
        this.uServiceImpl = uServiceImpl;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        final Object[] message = RPCUtils.decodeMessage(body);

        String nService = (String) message[0];

        if (nService != null) {
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                .Builder()
                .correlationId(properties.getCorrelationId())
                .build();

            Object result;
            String opResult = RabbitMQConstants.OK;
            try {
                final java.lang.reflect.Method method = nServicesHandlers.get(nService);
                if (method == null) {
                    throw new RuntimeException(rpcQueueName + "#" + nService + " nanoService not found!");
                }

                log.debug("invoking uSvc: " + rpcQueueName + "#" + nService);

                Object[] params = JSONUtils.decodeAsParams((byte[]) message[1], method.getGenericParameterTypes());
                result = ((CompletableFuture<?>) method.invoke(uServiceImpl, params)).join();
            } catch (Exception e) {
                log.debug("caught exception", e);

                opResult = RabbitMQConstants.KO;
                if (e.getCause() != null && e.getCause() instanceof GrisuException) {
                    result = ((GrisuException) e.getCause()).serialize();
                } else {
                    result = MapBuilder.instance().add(GrisuConstants.ERROR, e.toString()).add("errorCode", 500).build();
                }
            }

            try {
                channel.basicAck(envelope.getDeliveryTag(), false);
                channel.basicPublish("", properties.getReplyTo(), replyProps, RPCUtils.encodeMessage(opResult, JSONUtils.encode(result)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
