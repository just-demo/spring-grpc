package just.demo.client.unary;

import just.demo.proto.DemoRequest;
import just.demo.proto.DemoResponse;
import just.demo.proto.DemoServiceGrpc;
import just.demo.proto.DemoServiceGrpc.DemoServiceBlockingStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.grpc.client.GrpcChannelFactory;

@SpringBootApplication
public class UnaryDemoClient implements CommandLineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(UnaryDemoClient.class);

  private final GrpcChannelFactory channels;

  public UnaryDemoClient(GrpcChannelFactory channels) {
    this.channels = channels;
  }

  public static void main(String[] args) {
    System.exit(SpringApplication.exit(new SpringApplicationBuilder(UnaryDemoClient.class)
        .web(WebApplicationType.NONE)
        .properties("spring.grpc.server.enabled=false")
        .run(args)));
  }

  @Override
  public void run(String... args) {
    DemoServiceBlockingStub stub = DemoServiceGrpc.newBlockingStub(channels.createChannel("localhost:9090"));
    DemoResponse response = stub.unary(DemoRequest.newBuilder().setText("Demo request").build());
    LOG.info("response: {}", response.getText());
  }
}
