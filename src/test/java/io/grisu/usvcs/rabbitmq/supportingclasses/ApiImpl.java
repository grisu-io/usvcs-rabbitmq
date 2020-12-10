package io.grisu.usvcs.rabbitmq.supportingclasses;

import io.grisu.core.GrisuConstants;
import io.grisu.core.exceptions.GrisuException;
import io.grisu.core.utils.MapBuilder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class ApiImpl implements Api {

    private ThreadPoolExecutor executor;

    public ApiImpl() {
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);
    }

    @Override
    public CompletableFuture<String> echoService(String string) {
        return CompletableFuture.supplyAsync(() -> ">>>" + string);
    }

    @Override
    public CompletableFuture<String> longRunningService(Long millisecs, String id) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(millisecs);
            } catch (InterruptedException e) {
                System.out.println("e = " + e);
                throw new RuntimeException(e);
            }
            return id;
        }, executor);
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
