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

        final Class<?> uServiceHandler = Stream.of(uServiceImpl.getClass().getInterfaces())
                .filter(i -> i.getAnnotation(MicroService.class) != null).findFirst().orElseThrow(() ->
                        new RuntimeException("Service not annotated with @MicroService (" + uServiceImpl.getClass() + ")")
                );

        this.rpcQueueName = uServiceHandler.getAnnotation(MicroService.class).serviceQueue();

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
                // Left intentionally blank
            }
        }));

        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                final Object[] message = RPCUtils.decodeMessage(body);

                String nService = (String) message[0];

                if (nService != null) {
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                            .Builder()
                            .correlationId(properties.getCorrelationId())
                            .build();

                    try {
                        final java.lang.reflect.Method method = nServicesHandlers.get(nService);
                        if (method == null) {
                            throw new RuntimeException(rpcQueueName + "#" + nService + " nanoService not found!");
                        }

                        Object[] params = JSONUtils.decodeAsParams((byte[]) message[1], method.getGenericParameterTypes());

                        ((CompletableFuture<?>) method.invoke(uServiceImpl, params))
                                .whenComplete((res, ex) -> {
                                    if (ex != null) {
                                        reply(envelope, properties, replyProps, RabbitMQConstants.KO, handleException(ex));
                                    } else {
                                        reply(envelope, properties, replyProps, RabbitMQConstants.OK, res);
                                    }
                                });

                    } catch (Throwable e) {
                        reply(envelope, properties, replyProps, RabbitMQConstants.KO, handleException(e));
                    }

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

    private static Throwable findRootExceptionOrGrisuException(Throwable throwable) {
        while (throwable != null && !(throwable instanceof GrisuException) && throwable.getCause() != null && throwable != throwable.getCause()) {
            throwable = throwable.getCause();
        }
        return throwable;
    }

    private Object handleException(Throwable ex) {
        Throwable th;

        if (ex instanceof InvocationTargetException) {
            th = ((InvocationTargetException) ex).getTargetException();
        } else {
            th = ex;
        }

        Throwable cause = findRootExceptionOrGrisuException(th);

        if (cause instanceof GrisuException) {
            return ((GrisuException) cause).serialize();
        } else {
            return MapBuilder
                    .instance()
                    .add(GrisuConstants.ERROR_MESSAGE, th.toString())
                    .add(GrisuConstants.ERROR_CODE, RabbitMQConstants.ERROR_CODE).build();
        }
    }

    private void reply(Envelope envelope, AMQP.BasicProperties properties, AMQP.BasicProperties replyProps, String opResult, Object result) {
        try {
            channel.basicAck(envelope.getDeliveryTag(), false);
            channel.basicPublish("", properties.getReplyTo(), replyProps, RPCUtils.encodeMessage(opResult, JSONUtils.encode(result)));
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }

}
