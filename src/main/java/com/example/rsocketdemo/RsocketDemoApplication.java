package com.example.rsocketdemo;

import java.util.stream.IntStream;

import org.reactivestreams.Publisher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.SocketAcceptor;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class RsocketDemoApplication implements CommandLineRunner {

    public static final int PORT = 9999;

    public static void main(String[] args) {
        SpringApplication.run(RsocketDemoApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        runServer();
        runClient();
    }

    private void runServer() {
        System.out.println("run server");

        SocketAcceptor acceptor = new SocketAcceptor() {
            @Override
            public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
                System.out.println("server: accept");

                return Mono.just(new AbstractRSocket() {
                    @Override
                    public Flux<Payload> requestStream(Payload payload) {
                        System.out.println("server: execute requestStream");
                        return Flux.just(DefaultPayload.create(payload.getDataUtf8() + "_foo"),
                                DefaultPayload.create(payload.getDataUtf8() + "_bar"));
                    }

                    @Override
                    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                        System.out.println("server: execute requestChannel");
                        return Flux.from(payloads).map(p -> DefaultPayload.create(p.getDataUtf8() + "_buz"));
                    }
                });
            }
        };

        WebsocketServerTransport ws = WebsocketServerTransport.create(PORT);
        RSocketFactory.receive().acceptor(acceptor).transport(ws).start().subscribe();
    }

    private void runClient() {
        System.out.println("run client");

        WebsocketClientTransport ws = WebsocketClientTransport.create(PORT);
        RSocket client = RSocketFactory.connect().keepAlive().transport(ws).start().block();

        try {
            System.out.println("client: call requestStream");
            Flux<Payload> streamPayload = client.requestStream(DefaultPayload.create("DataFromClient"));
            streamPayload.doOnNext(p -> System.out.println(p.getDataUtf8())).blockLast();

            System.out.println("client: call requestChannel");
            Flux<Payload> channelPayload = client.requestChannel(
                    Flux.fromStream(IntStream.range(1, 10).boxed()).map(i -> DefaultPayload.create(String.valueOf(i))));
            channelPayload.doOnNext(p -> System.out.println(p.getDataUtf8())).blockLast();

        } finally {
            client.dispose();
        }
    }
}
