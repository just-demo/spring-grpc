package just.demo.server;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicInteger;
import just.demo.proto.DemoRequest;
import just.demo.proto.DemoResponse;
import just.demo.proto.DemoServiceGrpc.DemoServiceImplBase;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import static java.util.stream.IntStream.rangeClosed;

@Slf4j
@Service
@SuppressWarnings("LoggingSimilarMessage")
public class DemoServiceImpl extends DemoServiceImplBase {

  @Override
  public void unary(DemoRequest request, StreamObserver<DemoResponse> responseObserver) {
    log.info("request: {}", request.getText());
    responseObserver.onNext(DemoResponse.newBuilder().setText("Demo response").build());
    responseObserver.onCompleted();
  }

  @Override
  public void serverStream(DemoRequest request, StreamObserver<DemoResponse> responseObserver) {
    log.info("request: {}", request.getText());
    rangeClosed(1, 10).forEach(counter -> {
      responseObserver.onNext(DemoResponse.newBuilder().setText("Demo response " + counter).build());
    });
    responseObserver.onCompleted();
  }

  @Override
  public StreamObserver<DemoRequest> clientStream(StreamObserver<DemoResponse> responseObserver) {
    return new StreamObserver<>() {
      private final AtomicInteger counter = new AtomicInteger();

      @Override
      public void onNext(DemoRequest request) {
        log.info("request: {}", request.getText());
        counter.incrementAndGet();
      }

      @Override
      public void onError(Throwable t) {
        log.error("error: {}", t.getMessage());
      }

      @Override
      public void onCompleted() {
        responseObserver.onNext(DemoResponse.newBuilder()
            .setText("Received " + counter.get() + " requests")
            .build());
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  public StreamObserver<DemoRequest> bidirectionalStream(StreamObserver<DemoResponse> responseObserver) {
    return new StreamObserver<>() {
      @Override
      public void onNext(DemoRequest request) {
        log.info("request: {}", request.getText());
        responseObserver.onNext(DemoResponse.newBuilder().setText(request.getText()).build());
      }

      @Override
      public void onError(Throwable t) {
        log.error("error: {}", t.getMessage());
        responseObserver.onError(t);
      }

      @Override
      public void onCompleted() {
        log.info("completed");
        responseObserver.onCompleted();
      }
    };
  }
}
