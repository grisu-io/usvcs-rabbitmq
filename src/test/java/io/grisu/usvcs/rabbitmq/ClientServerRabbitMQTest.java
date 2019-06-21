package io.grisu.usvcs.rabbitmq;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.grisu.core.exceptions.GrisuException;
import io.grisu.usvcs.rabbitmq.supportingclasses.Api;
import io.grisu.usvcs.rabbitmq.supportingclasses.ApiImpl;
import io.grisu.usvcs.rabbitmq.supportingclasses.ApiStub;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClientServerRabbitMQTest {

    Channel channel;
    Api apiClient;

    ClientRabbitMQ client;
    ServerRabbitMQ server;

    @Before
    public void setup() throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new MockConnectionFactory();
        Connection conn = factory.newConnection();
        channel = conn.createChannel();

        client = new ClientRabbitMQ(channel);
        client.start();

        server = new ServerRabbitMQ(channel, 1, new ApiImpl());
        server.start();

        apiClient = new ApiStub(client);
    }

    @After
    public void teardown() throws InterruptedException, TimeoutException, IOException {
        client.stop();
        server.stop();
    }

    @Test
    public void shouldCompleteRoundTripWithCompletableFuture() throws IOException, TimeoutException, InterruptedException {
        CompletableFuture<String> cf = apiClient.echoService("repeat this");
        Assert.assertEquals(">>>repeat this", cf.join());
    }

    @Test
    public void shouldCompleteExceptionally() throws IOException, TimeoutException, InterruptedException {
        try {
            apiClient.errorService(7448).join();
            Assert.fail("Shouldn't pass here");
        } catch (Throwable t) {
            Assert.assertEquals(7448, (int) ((GrisuException) ExceptionUtils.getRootCause(t)).getErrorCode());
        }
    }

}
