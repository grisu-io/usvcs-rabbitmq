package io.grisu.usvcs.rabbitmq;

import com.rabbitmq.client.*;
import io.grisu.core.GrisuConstants;
import io.grisu.core.exceptions.GrisuException;
import io.grisu.core.utils.MapBuilder;
import io.grisu.pojo.utils.JSONUtils;
import io.grisu.usvcs.annotations.MicroService;
import io.grisu.usvcs.annotations.NanoService;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class ServerRabbitMQ {

    private static final long SLEEP_MILLISECS = 1000;

    private final Channel channel;
    private final String rpcQueueName;
    private final int concurrency;
    private final Map<String, java.lang.reflect.Method> nServicesHandlers;

    private volatile AtomicBoolean running;
    private String consumerTag;

    final Consumer consumer;

    public ServerRabbitMQ(Channel channel, int concurrency, Object uServiceImpl) {
        this.channel = channel;
        this.concurrency = concurrency;

        final Class uServiceHandler = Stream.of(uServiceImpl.getClass().getInterfaces())
            .filter(i -> i.getAnnotation(MicroService.class) != null).findFirst().orElseThrow(() ->
                new RuntimeException("Service not annotated with @MicroService (" + uServiceImpl.getClass() + ")")
            );

        this.rpcQueueName = ((MicroService) uServiceHandler.getAnnotation(MicroService.class)).serviceQueue();

        nServicesHandlers = new HashMap<>();
        Stream.of(uServiceHandler.getMethods())
            .forEach(m -> {
                final NanoService nanoServiceAnnotation = m.getAnnotation(NanoService.class);
                if (nanoServiceAnnotation != null) {
                    nServicesHandlers.put(nanoServiceAnnotation.name(), m);
                }
            });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                this.stop();
            } catch (Exception e) {
            }
        }));

        consumer = new DefaultConsumer(channel) {
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

                        Object[] params = JSONUtils.decodeAsParams((byte[]) message[1], method.getGenericParameterTypes());
                        result = ((CompletableFuture<?>) method.invoke(uServiceImpl, params)).join();
                    } catch (Exception e) {
                        opResult = RabbitMQConstants.KO;

                        Throwable th;

                        if (e instanceof InvocationTargetException) {
                            th = ((InvocationTargetException)e).getTargetException();
                        } else {
                            th = e;
                        }

                        if (th instanceof GrisuException) {
                            result = ((GrisuException) th).serialize();
                        } else {
                            result = MapBuilder
                                .instance()
                                .add(GrisuConstants.ERROR_MESSAGE, th.toString())
                                .add(GrisuConstants.ERROR_CODE, RabbitMQConstants.ERROR_CODE).build();
                        }
                    }

                    channel.basicAck(envelope.getDeliveryTag(), false);
                    channel.basicPublish("", properties.getReplyTo(), replyProps, RPCUtils.encodeMessage(opResult, JSONUtils.encode(result)));
                }
            }
        };
    }

    public void start() throws IOException, InterruptedException {
        this.running = new AtomicBoolean(true);

        channel.queueDeclare(rpcQueueName, false, false, false, null);
        if (concurrency > 0) {
            channel.basicQos(concurrency);
        }

        consumerTag = channel.basicConsume(rpcQueueName, false, consumer);

    }

    public void stop() throws IOException, TimeoutException, InterruptedException {
        this.running.set(false);

        channel.basicCancel(consumerTag);
        Thread.sleep(SLEEP_MILLISECS);

        Connection connection = channel.getConnection();
        channel.close();
        connection.close();
    }

}
