package just.demo;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication(scanBasePackages = "just.demo.service")
public class SpringGrpcApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(SpringGrpcApplication.class)
                .profiles("server")
                .run(args);
    }

}
