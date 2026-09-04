package dev.thompgt.enricher.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point. Wiring, configuration and metrics live in this module; the
 * pipeline itself lives in {@code enricher-core} and {@code enricher-kafka}, which
 * know nothing about Spring (CLAUDE.md invariant 3).
 */
@SpringBootApplication
public class EnricherApplication {

    public static void main(String[] args) {
        SpringApplication.run(EnricherApplication.class, args);
    }
}
