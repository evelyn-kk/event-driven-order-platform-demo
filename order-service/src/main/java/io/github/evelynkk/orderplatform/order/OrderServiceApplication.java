package io.github.evelynkk.orderplatform.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Scanning is widened to the platform root so the shared outbox and deduplication components in
 * {@code platform-messaging} are picked up alongside this service's own beans, entities, and
 * repositories. Boot derives its default scan from this class's own package, which would miss them.
 */
@SpringBootApplication(scanBasePackages = "io.github.evelynkk.orderplatform")
@EnableJpaRepositories(basePackages = "io.github.evelynkk.orderplatform")
@EntityScan(basePackages = "io.github.evelynkk.orderplatform")
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
