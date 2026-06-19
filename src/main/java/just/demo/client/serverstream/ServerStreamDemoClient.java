package just.demo.client.serverstream;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
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
public class ServerStreamDemoClient implements CommandLineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ServerStreamDemoClient.class);

  private final GrpcChannelFactory channels;

  public ServerStreamDemoClient(GrpcChannelFactory channels) {
    this.channels = channels;
  }

  public static void main(String[] args) {
    System.exit(SpringApplication.exit(new SpringApplicationBuilder(ServerStreamDemoClient.class)
        .web(WebApplicationType.NONE)
        .properties("spring.grpc.server.enabled=false")
        .run(args)));
  }

  @Override
  public void run(String... args) throws InterruptedException {
    CountDownLatch done = new CountDownLatch(1);
    DemoServiceStub stub = DemoServiceGrpc.newStub(channels.createChannel("localhost:9090"));
    StreamObserver<DemoResponse> responseObserver = new StreamObserver<>() {
      @Override
      public void onNext(DemoResponse response) {
        LOG.info("response: {}", response.getText());
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
    };

    stub.serverStream(DemoRequest.newBuilder().setText("Demo request").build(), responseObserver);
    done.await();
  }
}
