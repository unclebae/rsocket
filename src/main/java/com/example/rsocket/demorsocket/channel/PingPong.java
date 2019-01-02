package com.example.rsocket.demorsocket.channel;

import io.rsocket.*;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.log4j.Log4j2;
import org.reactivestreams.Publisher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * 핑퐁 어플리케이션은 피어들간에 Ping/Pong 을 서로 주고받는 예제입니다.
 *
 */
@SpringBootApplication
public class PingPong {

    /**
     * 핑과 퐁을 주고 받는 메소드입니다.
     * 핑이 오면 퐁을 응답하고, 퐁이오면 다시 핑을 응답하는 메소드입니다.
     * @param in
     * @return
     */
    static String reply(String in) {
        if (in.equalsIgnoreCase("ping")) return "pong";
        if (in.equalsIgnoreCase("pong")) return "ping";
        throw new IllegalArgumentException("incoming value must be either 'ping' or 'pong'");
    }

    public static void main(String[] args) {
        SpringApplication.run(PingPong.class, args);
    }
}

@Log4j2
@Component
class Ping implements Ordered, ApplicationListener<ApplicationReadyEvent> {

    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        log.info("starting " + this.getClass().getName());

        RSocketFactory
                .connect()
                .transport(TcpClientTransport.create(7000))
                .start()
                .flatMapMany (socket ->
                        socket.requestChannel(  //  1초마다 시그널을 생성하여 채널로 요청 보낸다.
                                Flux.interval(Duration.ofSeconds(1)).map(i -> DefaultPayload.create("ping"))
                        )
                        .map(payload -> payload.getDataUtf8())
                        .doOnNext(str -> log.info("received " + str + " in " + getClass().getName()))
                        .take(10)   // 10개만 처리합니다.
                        .doFinally(signal -> socket.dispose())  // 소켓 내용을 처리한다.
                )
                .then()
                .block();   //  메시지를 보내고 응답을 대기합니다.
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}

/**
 * 핑을 보내오면 퐁을 응답합니다.
 */
@Log4j2
@Component
class Pong implements SocketAcceptor, Ordered, ApplicationListener<ApplicationReadyEvent> {

    /**
     * 어플리케이션이 실행되고, 서버와 커넥션을 맺습니다.
     * 서버와 커넥션을 맺고나서 해당 수신을 받기위해 대기합니다.
     * @param applicationReadyEvent
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        RSocketFactory
                .receive()
                .acceptor(this)
                .transport(TcpServerTransport.create(7000))
                .start()
                .subscribe();
    }

    /**
     * 우선순위는 먼저 받을 준비부터 합니다. 그러크로 퐁이 우선순위가 높습니다.
     * @return
     */
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    /**
     * SocketAcceptor 에 대한 구현체를 구현한 메소드 입니다 .
     * 소켓에 채널을 열고 피어가 보내온 요청을 수신 받습니다.
     * @param connectionSetupPayload
     * @param rSocket
     * @return
     */
    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload connectionSetupPayload, RSocket rSocket) {
        AbstractRSocket rs = new AbstractRSocket() {
            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                return Flux
                        .from(payloads)
                        .map(Payload::getDataUtf8)
                        .doOnNext(str -> log.info("received " + str + " in " + getClass()))
                        .map(PingPong::reply)
                        .map(DefaultPayload::create)
                        ;
            }
        };

        return Mono.just(rs);
    }
}

