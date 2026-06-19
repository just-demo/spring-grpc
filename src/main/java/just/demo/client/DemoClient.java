package just.demo.client;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import just.demo.proto.DemoRequest;
import just.demo.proto.DemoResponse;
import just.demo.proto.DemoServiceGrpc.DemoServiceBlockingStub;
import just.demo.proto.DemoServiceGrpc.DemoServiceStub;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.grpc.client.ImportGrpcClients;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.IntStream.rangeClosed;

@Slf4j
@RequiredArgsConstructor
@SpringBootApplication
@ImportGrpcClients(target = "demo", types = {DemoServiceBlockingStub.class, DemoServiceStub.class})
@SuppressWarnings("LoggingSimilarMessage")
public class DemoClient implements CommandLineRunner {

    private final DemoServiceBlockingStub blockingStub;
    private final DemoServiceStub asyncStub;

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(new SpringApplicationBuilder(DemoClient.class)
                .profiles("client")
                .run(args)));
    }

    @Override
    public void run(String... args) throws InterruptedException {
        log.info("Unary...");
        runUnary(blockingStub);
        log.info("Server streaming...");
        runServerStream(asyncStub);
        log.info("Client streaming...");
        runClientStream(asyncStub);
        log.info("Bidirectional streaming...");
        runBidirectionalStream(asyncStub);
    }

    private static void runUnary(DemoServiceBlockingStub stub) {
        DemoResponse response = stub.unary(DemoRequest.newBuilder().setText("Demo request").build());
        log.info("response: {}", response.getText());
    }

    private static void runServerStream(DemoServiceStub stub) throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        stub.serverStream(DemoRequest.newBuilder().setText("Demo request").build(), new StreamObserver<>() {
            @Override
            public void onNext(DemoResponse response) {
                log.info("response: {}", response.getText());
            }

            @Override
            public void onError(Throwable t) {
                log.error("error: {}", t.getMessage());
                done.countDown();
            }

            @Override
            public void onCompleted() {
                log.info("completed");
                done.countDown();
            }
        });
        done.await();
    }

    private static void runClientStream(DemoServiceStub stub) throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<DemoRequest> requestObserver = stub.clientStream(new StreamObserver<>() {
            @Override
            public void onNext(DemoResponse response) {
                log.info("response: {}", response.getText());
            }

            @Override
            public void onError(Throwable t) {
                log.error("error: {}", t.getMessage());
                done.countDown();
            }

            @Override
            public void onCompleted() {
                log.info("completed");
                done.countDown();
            }
        });
        rangeClosed(1, 10).forEach(counter ->
                requestObserver.onNext(DemoRequest.newBuilder().setText("Demo request " + counter).build()));
        requestObserver.onCompleted();
        done.await();
    }

    private static void runBidirectionalStream(DemoServiceStub stub) throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger();
        stub.bidirectionalStream(new ClientResponseObserver<DemoRequest, DemoResponse>() {
            private ClientCallStreamObserver<DemoRequest> requestStream;

            @Override
            public void beforeStart(ClientCallStreamObserver<DemoRequest> requestStream) {
                this.requestStream = requestStream;
                requestStream.setOnReadyHandler(this::next);
            }

            @Override
            public void onNext(DemoResponse response) {
                log.info("response: {}", response.getText());
                next();
            }

            @Override
            public void onError(Throwable t) {
                log.error("error: {}", t.getMessage());
                done.countDown();
            }

            @Override
            public void onCompleted() {
                log.info("completed");
                done.countDown();
            }

            private void next() {
                if (counter.incrementAndGet() > 10) {
                    requestStream.onCompleted();
                } else {
                    requestStream.onNext(DemoRequest.newBuilder().setText("Demo request " + counter.get()).build());
                }
            }
        });
        done.await();
    }
}
