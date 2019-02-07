package io.grisu.usvcs.rabbitmq.supportingclasses;

import java.util.concurrent.CompletableFuture;

import io.grisu.core.exceptions.GrisuException;
import io.grisu.core.utils.MapBuilder;

public class ApiImpl implements Api {

    @Override
    public CompletableFuture<String> echoService(String string) {
        return CompletableFuture.supplyAsync(() -> ">>>" + string);
    }

    @Override
    public CompletableFuture<String> errorService(Integer errorToReturn) {
        throw GrisuException.build(MapBuilder.instance().add(GrisuException.ERROR_CODE, errorToReturn).build());
    }

}
