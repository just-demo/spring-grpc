package just.demo.client.bidirectional;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import just.demo.proto.DemoRequest;
import just.demo.proto.DemoResponse;
import just.demo.proto.DemoServiceGrpc;
import just.demo.proto.DemoServiceGrpc.DemoServiceStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.grpc.client.GrpcChannelFactory;

@SpringBootApplication
public class BidirectionalStreamDemoClient implements CommandLineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(BidirectionalStreamDemoClient.class);

  private final GrpcChannelFactory channels;

  public BidirectionalStreamDemoClient(GrpcChannelFactory channels) {
    this.channels = channels;
  }

  public static void main(String[] args) {
    System.exit(SpringApplication.exit(new SpringApplicationBuilder(BidirectionalStreamDemoClient.class)
        .web(WebApplicationType.NONE)
        .properties("spring.grpc.server.enabled=false")
        .run(args)));
  }

  @Override
  public void run(String... args) throws InterruptedException {
    CountDownLatch done = new CountDownLatch(1);
    DemoServiceStub stub = DemoServiceGrpc.newStub(channels.createChannel("localhost:9090"));
    AtomicInteger counter = new AtomicInteger();
    ClientResponseObserver<DemoRequest, DemoResponse> responseObserver = new ClientResponseObserver<>() {
      private ClientCallStreamObserver<DemoRequest> requestStream;

      @Override
      public void beforeStart(ClientCallStreamObserver<DemoRequest> requestStream) {
        this.requestStream = requestStream;
        requestStream.setOnReadyHandler(this::next);
      }

      @Override
      public void onNext(DemoResponse response) {
        LOG.info("response: {}", response.getText());
        next();
      }

      @Override
      public void onError(Throwable t) {
        LOG.error("error: {}", t.getMessage());
        done.countDown();
      }

      @Override
      public void onCompleted() {
        LOG.info("completed");
        done.countDown();
      }

      private void next() {
        if (counter.incrementAndGet() > 10) {
          requestStream.onCompleted();
        } else {
          requestStream.onNext(DemoRequest.newBuilder().setText("Demo request " + counter.get()).build());
        }
      }
    };

    stub.bidiStream(responseObserver);
    done.await();
  }
}
