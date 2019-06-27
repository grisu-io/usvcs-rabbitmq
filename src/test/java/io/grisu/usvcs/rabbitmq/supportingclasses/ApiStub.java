package io.grisu.usvcs.rabbitmq.supportingclasses;

import java.util.concurrent.CompletableFuture;

import io.grisu.usvcs.AbstractStub;
import io.grisu.usvcs.Client;

public class ApiStub extends AbstractStub implements Api {

    public ApiStub(Client client) {
        super(client);
    }

    @Override
    public CompletableFuture<String> echoService(String string) {
        return super.call(new Object() {
        }.getClass().getEnclosingMethod(), string);
    }

    @Override
    public CompletableFuture<String> errorServiceGrisuException(Integer errorToReturn) {
        return super.call(new Object() {
        }.getClass().getEnclosingMethod(), errorToReturn);
    }

    @Override
    public CompletableFuture<String> errorServiceNonGrisuException() {
        return super.call(new Object() {
        }.getClass().getEnclosingMethod());
    }
}
