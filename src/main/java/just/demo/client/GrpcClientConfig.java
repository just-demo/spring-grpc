package just.demo.client;

import io.grpc.Channel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.client.GrpcChannelFactory;

@Configuration
class GrpcClientConfig {

    @Bean
    Channel demoChannel(GrpcChannelFactory channels) {
        return channels.createChannel("demo");
    }
}
