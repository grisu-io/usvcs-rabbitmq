package io.grisu.usvcs.rabbitmq.supportingclasses;

import java.util.concurrent.CompletableFuture;

import io.grisu.usvcs.annotations.MicroService;
import io.grisu.usvcs.annotations.NanoService;

@MicroService(serviceQueue = "api")
public interface Api {

    @NanoService(name = "echo")
    CompletableFuture<String> echoService(String string);

    @NanoService(name = "error")
    CompletableFuture<String> errorService(Integer errorToReturn);

}
