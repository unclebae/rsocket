package com.example.rsocket.demorsocket.rr;

import io.rsocket.*;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

/**
 * MONO 참조
 * https://projectreactor.io/docs/core/snapshot/api/reactor/core/publisher/Mono.html#flatMapMany-java.util.function.Function-
 */

/**
 * RequestResponse 클래스는 Producer 는 소켓을 열고 대기하고, Consumer 는 해당 데이터를 수신받아 출력하는 예제입니다.
 * RSocket 을 이용하여 Produce > Consume 를 수행하는 테스트를 확인합시다.
 */
@SpringBootApplication
public class RequestResponse {

    public static void main(String args[]) {
        SpringApplication.run(RequestResponse.class, args);
    }
}

@Component
class Producer implements Ordered, ApplicationListener<ApplicationReadyEvent> {

    /**
     * Ordered 의 구현 메소드입니다.
     * Producer 이 먼저 메시지를 만들어야하기 때문에 우선순위를 HIGHEST_PRECEDENCE로 두었습니다.
     * @return 결과값은 상수입니다.
     */
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    /**
     * 1초 딜레이후 메시지를 생성하는 메소드 입니다.
     * 메소드는 Hello 이름 @ 클래스 이름 이 됩니다.
     * @param name 출력할 이름
     * @return 반환값은 Flux 문자를 반환합니다. 하나이상의 객체 스트림을 전송할때는 Flux 를 이용합니다.
     */
    Flux<String> notifications(String name) {
        // 스트림 객체를 생성합니다.
        return Flux.fromStream(Stream.generate(() -> "Hello " + name + "@ " + Instant.now().toString()))
                .delayElements(Duration.ofSeconds(1));  // 1초 딜레이 시킴
    }

    /**
     * ApplicationListener 의 구현체입니다. 이 중에서 ApplicationReadyEvent 가 발생하면 해당 이벤트를 캐치합니다.
     * @param applicationReadyEvent
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {

        /**
         * 완벽한 Full Duplex 소켓 처리를 수행합니다.
         * 피어에 요청을 보내거나 피어로부터 응답을 수신받는 역할을 수행합니다.
         */
        SocketAcceptor scoketAcceptor = new SocketAcceptor() {

            // connectionSetupPayload : 클라이언트가 보낸 설정 을 셋업합니다.
            // rSocket : 피어와 통신할 소켓입니다.
            @Override
            public Mono<RSocket> accept(ConnectionSetupPayload connectionSetupPayload, RSocket rSocket) {

                // 피어와 통신할 방식을 지정합니다.
                // 여기에서는 스트림을 전송하는 기능을 구현합니다.
                AbstractRSocket abstractRSocket = new AbstractRSocket() {

                    @Override
                    public Flux<Payload> requestStream(Payload payload) {
                        final String name = payload.getDataUtf8();
                        return notifications(name).map(DefaultPayload::create);
                    }
                };

                return Mono.just(abstractRSocket);

            }
        };

        /**
         * Tcp를 이용하여 서버에 전송을 하고자 할때 이 객체를 이용합니다.
         * 아래 예제는 7000을 이용하여 로컬 호스트에 접속을 하게 됩니다.
         */
        final TcpServerTransport transport = TcpServerTransport.create(7000);

        /**
         * RSocketFactory 은 소켓을 이용하여 데이터를 송신/수신하기 위한 추상화 레벨을 제공하는 객체입니다.
         * 아래는 서버 접속을 수행하고,
         */
        RSocketFactory
                .receive()
                .acceptor(scoketAcceptor)
                .transport(transport)
                .start()
                .block();   //  다음 시그널이 올때까지 대기한다.
    }
}

@Log4j2
@Component
class Consumer implements Ordered, ApplicationListener<ApplicationReadyEvent> {

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    /**
     * 어플리케이션이 수행되면 해당 이벤트를 받아서 서버와 커넥션을 맺습니다.
     * 이후 수신받은 내용은 flatMapMany 를 이용하여 요청을 스트림으로 내용들을 던집니다.
     * @param applicationReadyEvent
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        RSocketFactory
                .connect()
                .transport(TcpClientTransport.create(7000))
                .start()
                .flatMapMany(sender ->
                        sender.requestStream(DefaultPayload.create("Spring Tips"))
                        .map(Payload::getDataUtf8)
//                        .doOnNext(log::info)
                )
                .subscribe(result -> log.info("Processing new Result " + result));
    }
}