package io.grisu.usvcs.rabbitmq.supportingclasses;

import java.util.concurrent.CompletableFuture;

import io.grisu.usvcs.annotations.MicroService;
import io.grisu.usvcs.annotations.NanoService;

@MicroService(serviceQueue = "api")
public interface Api {

    @NanoService(name = "echo")
    CompletableFuture<String> echoService(String string);

    @NanoService(name = "long-running")
    CompletableFuture<String> longRunningService(Long millisecs, String id);

    @NanoService(name = "error-grisu")
    CompletableFuture<String> errorServiceGrisuException(Integer errorToReturn);

    @NanoService(name = "error-non-grisu")
    CompletableFuture<String> errorServiceNonGrisuException();

    @NanoService(name = "error-completion-exception-grisu")
    CompletableFuture<String> errorServiceCompletionExceptionGrisuException(Integer errorToReturn);

    @NanoService(name = "error-completion-exception-grisu")
    CompletableFuture<String> errorServiceCompletionExceptionGrisuException_Wrap2(Integer errorToReturn);

}
