package io.ratelimit4j.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * A runnable gateway that puts the limiters in front of real HTTP traffic.
 *
 * <p>Exists so the library can be demonstrated rather than only described: start it, point a load
 * generator at it, and watch the 429s and {@code RateLimit-*} headers appear. See the README for the
 * {@code curl} and {@code hey} invocations.
 *
 * <pre>{@code
 * mvn -pl ratelimit4j-gateway -am spring-boot:run
 * curl -i localhost:8080/api/echo
 * }</pre>
 */
@SpringBootApplication
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}
