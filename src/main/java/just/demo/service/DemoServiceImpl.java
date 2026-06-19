package just.demo.service;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicInteger;
import just.demo.proto.DemoRequest;
import just.demo.proto.DemoResponse;
import just.demo.proto.DemoServiceGrpc.DemoServiceImplBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import static java.util.stream.IntStream.rangeClosed;

@Service
@SuppressWarnings("LoggingSimilarMessage")
public class DemoServiceImpl extends DemoServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(DemoServiceImpl.class);

  @Override
  public void unary(DemoRequest request, StreamObserver<DemoResponse> responseObserver) {
    LOG.info("request: {}", request.getText());
    responseObserver.onNext(DemoResponse.newBuilder().setText("Demo response").build());
    responseObserver.onCompleted();
  }

  @Override
  public void serverStream(DemoRequest request, StreamObserver<DemoResponse> responseObserver) {
    LOG.info("request: {}", request.getText());
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
        LOG.info("request: {}", request.getText());
        counter.incrementAndGet();
      }

      @Override
      public void onError(Throwable t) {
        LOG.error("error: {}", t.getMessage());
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
  public StreamObserver<DemoRequest> bidiStream(StreamObserver<DemoResponse> responseObserver) {
    return new StreamObserver<>() {
      @Override
      public void onNext(DemoRequest request) {
        LOG.info("request: {}", request.getText());
        responseObserver.onNext(DemoResponse.newBuilder().setText(request.getText()).build());
      }

      @Override
      public void onError(Throwable t) {
        LOG.error("error: {}", t.getMessage());
        responseObserver.onError(t);
      }

      @Override
      public void onCompleted() {
        LOG.info("completed");
        responseObserver.onCompleted();
      }
    };
  }
}
