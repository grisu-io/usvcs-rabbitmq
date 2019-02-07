package io.grisu.usvcs.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.grisu.usvcs.annotations.MicroService;
import io.grisu.usvcs.annotations.NanoService;
import io.grisu.usvcs.rabbitmq.consumers.ServerDefault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerRabbitMQ {

    private static final long SLEEP_MILLISECS = 1000;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Channel channel;
    private final String rpcQueueName;
    private final int concurrency;
    private final Map<String, java.lang.reflect.Method> nServicesHandlers;

    private volatile AtomicBoolean running;
    private String consumerTag;

    final ServerDefault serverDefault;

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

        serverDefault = new ServerDefault(channel, rpcQueueName, nServicesHandlers, uServiceImpl);
    }

    public void start() throws IOException, InterruptedException {
        log.info("Ready...");
        this.running = new AtomicBoolean(true);

        channel.queueDeclare(rpcQueueName, false, false, false, null);
        if (concurrency > 0) {
            channel.basicQos(concurrency);
        }

        consumerTag = channel.basicConsume(rpcQueueName, false, serverDefault);

    }

    public void stop() throws IOException, TimeoutException, InterruptedException {
        log.info("Initiating graceful shutdown...");
        this.running.set(false);

        channel.basicCancel(consumerTag);
        Thread.sleep(SLEEP_MILLISECS);

        Connection connection = channel.getConnection();
        channel.close();
        connection.close();
    }

}
