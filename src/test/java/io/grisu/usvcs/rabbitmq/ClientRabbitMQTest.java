package io.grisu.usvcs.rabbitmq;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import io.grisu.pojo.utils.JSONUtils;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class ClientRabbitMQTest {

    @Rule
    public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

    @Mock
    Channel channel;

    @Mock
    AMQP.Queue.DeclareOk queue;

    @Test
    public void shouldProcessOutgoingMessage() throws IOException {
        String uSvcName = "usvc-name";
        String nSvcName = "nsvc-name";

        Mockito.doReturn(queue)
            .when(channel)
            .queueDeclare();

        Mockito.doReturn(uSvcName)
            .when(queue)
            .getQueue();

        ClientRabbitMQ clientRabbitMQ = new ClientRabbitMQ(channel);

        clientRabbitMQ.send(uSvcName, nSvcName, String.class, 5L, "Test string");

        InOrder inOrder = Mockito.inOrder(channel, queue);
        inOrder.verify(channel).queueDeclare();
        inOrder.verify(queue).getQueue();
        inOrder.verify(channel).basicPublish(eq(""), eq(uSvcName), any(AMQP.BasicProperties.class), eq(RPCUtils.encodeMessage(nSvcName, JSONUtils.encode(new Object[] { 5L, "Test string" }))));
        inOrder.verifyNoMoreInteractions();
    }


    @Test
    public void shouldProcessReplyMessage() throws IOException {
        String uSvcName = "usvc-name";
        String nSvcName = "nsvc-name";

        Mockito.doReturn(queue)
            .when(channel)
            .queueDeclare();

        Mockito.doReturn(uSvcName)
            .when(queue)
            .getQueue();

        ClientRabbitMQ clientRabbitMQ = new ClientRabbitMQ(channel);
        clientRabbitMQ.start();

        InOrder inOrder = Mockito.inOrder(channel, queue);
        inOrder.verify(channel).basicConsume(any(String.class), eq(true), any(DefaultConsumer.class));
        inOrder.verifyNoMoreInteractions();
    }

}
