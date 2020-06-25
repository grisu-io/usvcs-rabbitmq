package io.grisu.usvcs.rabbitmq.supportingclasses;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import io.grisu.core.GrisuConstants;
import io.grisu.core.exceptions.GrisuException;
import io.grisu.core.utils.MapBuilder;

public class ApiImpl implements Api {

    @Override
    public CompletableFuture<String> echoService(String string) {
        return CompletableFuture.supplyAsync(() -> ">>>" + string);
    }

    @Override
    public CompletableFuture<String> errorServiceGrisuException(Integer errorToReturn) {
        throw GrisuException.build(MapBuilder.instance().add(GrisuConstants.ERROR_CODE, errorToReturn).build());
    }

    @Override
    public CompletableFuture<String> errorServiceNonGrisuException() {
        throw new RuntimeException("All Your Base Are Belong To Us");
    }

    @Override
    public CompletableFuture<String> errorServiceCompletionExceptionGrisuException(Integer errorToReturn) {
        GrisuException grisuException = GrisuException.build(MapBuilder.instance().add(GrisuConstants.ERROR_CODE, errorToReturn).build());
        throw new CompletionException(grisuException);
    }

    @Override
    public CompletableFuture<String> errorServiceCompletionExceptionGrisuException_Wrap2(Integer errorToReturn) {
        GrisuException grisuException = GrisuException.build(MapBuilder.instance().add(GrisuConstants.ERROR_CODE, errorToReturn).build());
        CompletionException inner = new CompletionException(grisuException);
        CompletionException outer = new CompletionException(inner);
        throw outer;
    }
}
