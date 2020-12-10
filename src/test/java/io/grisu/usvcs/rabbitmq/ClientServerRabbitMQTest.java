package io.grisu.usvcs.rabbitmq;

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

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

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
    public void shouldCompleteRoundTripWithCompletableFuture() {
        CompletableFuture<String> cf = apiClient.echoService("repeat this");
        Assert.assertEquals(">>>repeat this", cf.join());
    }

    @Test
    public void shouldRunParallelTasks() throws Throwable {
        Long start = new Date().getTime();
        CompletableFuture<String> cf1 = apiClient.longRunningService(1500L, "1");
        CompletableFuture<String> cf2 = apiClient.longRunningService(1500L, "2");
        CompletableFuture<String> cf3 = apiClient.longRunningService(1500L, "3");
        CompletableFuture.allOf(cf1, cf2, cf3).join();
        Long end = new Date().getTime();
        Assert.assertEquals("1", cf1.get());
        Assert.assertEquals("2", cf2.get());
        Assert.assertTrue(end - start < 2000);
    }

    @Test
    public void shouldCompleteExceptionally_GrisuException() {
        try {
            apiClient.errorServiceGrisuException(7448).join();
            Assert.fail("Shouldn't pass here");
        } catch (Throwable t) {
            Assert.assertEquals(7448, (int) ((GrisuException) ExceptionUtils.getRootCause(t)).getErrorCode());
        }
    }

    @Test
    public void shouldCompleteExceptionally_CompletionExceptionWrappingGrisuException() {
        try {
            apiClient.errorServiceCompletionExceptionGrisuException(7448).join();
            Assert.fail("Shouldn't pass here");
        } catch (Throwable t) {
            Assert.assertEquals(7448, (int) ((GrisuException) ExceptionUtils.getRootCause(t)).getErrorCode());
        }
    }

    @Test
    public void shouldCompleteExceptionally_CompletionExceptionWrappingGrisuException_l2() {
        try {
            apiClient.errorServiceCompletionExceptionGrisuException_Wrap2(7448).join();
            Assert.fail("Shouldn't pass here");
        } catch (Throwable t) {
            GrisuException grisuException = ((GrisuException) ExceptionUtils.getRootCause(t));
            Assert.assertEquals(7448, (int) grisuException.getErrorCode());
        }
    }

    @Test
    public void shouldCompleteExceptionally_NonGrisuException() {
        try {
            apiClient.errorServiceNonGrisuException().join();
            Assert.fail("Shouldn't pass here");
        } catch (Throwable t) {
            assertNotNull(t.getCause());
            Assert.assertEquals(500, (int) ((GrisuException) ExceptionUtils.getRootCause(t)).getErrorCode());

            assertTrue(t.getCause() instanceof GrisuException);
            GrisuException grisuException = (GrisuException) t.getCause();
            assertEquals("java.lang.RuntimeException: All Your Base Are Belong To Us", grisuException.getErrorMessage());
        }
    }

}
