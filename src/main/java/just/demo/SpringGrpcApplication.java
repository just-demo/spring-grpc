package just.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "just.demo.service")
public class SpringGrpcApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringGrpcApplication.class, args);
    }

}
