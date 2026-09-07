package io.ratelimit4j.gateway;

import java.time.Instant;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Endpoints that do nothing but succeed, so that what is being observed is the limiter and not the
 * handler behind it.
 *
 * <p>The paths correspond to the rules in {@code application.yml} and are chosen to show the two
 * cases that matter: a tight limit on an endpoint where abuse is expensive
 * ({@code /api/auth/login}), and a loose one on ordinary traffic ({@code /api/**}).
 */
@RestController
@RequestMapping("/api")
public class DemoController {

    @GetMapping("/echo")
    public Map<String, Object> echo() {
        return Map.of("message", "ok", "at", Instant.now().toString());
    }

    /**
     * Stands in for an expensive or security-sensitive endpoint. Limited far more tightly than
     * {@code /api/**} — five attempts a minute, which is the kind of limit that only makes sense when
     * enforced exactly.
     */
    @PostMapping("/auth/login")
    public Map<String, Object> login() {
        return Map.of("message", "authenticated", "at", Instant.now().toString());
    }

    /** Unlimited, to show that a path matching no rule passes straight through. */
    @GetMapping("/unlimited")
    public Map<String, Object> unlimited() {
        return Map.of("message", "no rule matches this path", "at", Instant.now().toString());
    }
}
