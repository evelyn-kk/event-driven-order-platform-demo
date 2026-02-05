package io.github.evelynkk.orderplatform.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = "io.github.evelynkk.orderplatform")
@EnableJpaRepositories(basePackages = "io.github.evelynkk.orderplatform")
@EntityScan(basePackages = "io.github.evelynkk.orderplatform")
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
