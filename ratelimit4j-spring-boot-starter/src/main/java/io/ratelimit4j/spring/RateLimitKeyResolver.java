package io.ratelimit4j.spring;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Decides which key a request is limited against — the single most consequential choice in deploying
 * a rate limiter.
 *
 * <p>Get the dimension wrong and the limiter either does nothing useful or breaks legitimate traffic:
 *
 * <ul>
 *   <li><strong>By IP</strong> punishes everyone behind one NAT or corporate proxy together, and is
 *       trivially evaded by an attacker with a pool of addresses. It is nonetheless the only option
 *       for unauthenticated endpoints.
 *   <li><strong>By API key or authenticated principal</strong> is the right dimension whenever it is
 *       available: it maps to who is actually responsible for the traffic and cannot be rotated for
 *       free.
 *   <li><strong>By a client-supplied header</strong> is only as trustworthy as the header. If anything
 *       upstream lets a client set it, the client can set it to a fresh value per request and the
 *       limiter is decorative.
 * </ul>
 *
 * <p>Implementations must be cheap and must never return null; return a constant such as
 * {@code "anonymous"} when no better key exists, so that unattributable traffic still shares one
 * bucket rather than escaping the limit entirely.
 */
@FunctionalInterface
public interface RateLimitKeyResolver {

    /**
     * @return the limiter key for {@code request}; never null
     */
    String resolve(HttpServletRequest request);

    /**
     * Keys by the immediate peer address.
     *
     * <p>Deliberately does <em>not</em> consult {@code X-Forwarded-For}. That header is client-supplied
     * and, unless a proxy you control overwrites it, an attacker can put anything in it and mint a
     * fresh limiter key per request. Use {@link #byTrustedForwardedFor} only when a trusted proxy
     * rewrites the header.
     */
    static RateLimitKeyResolver byRemoteAddress() {
        return request -> {
            String address = request.getRemoteAddr();
            return address == null ? "unknown" : address;
        };
    }

    /**
     * Keys by the left-most entry of {@code X-Forwarded-For}, falling back to the peer address.
     *
     * <p>Only safe behind a proxy that <em>overwrites</em> the header rather than appending to it. If
     * the client's own value survives to the application, this resolver hands the client control of
     * its own limiter key.
     */
    static RateLimitKeyResolver byTrustedForwardedFor() {
        return request -> {
            String forwarded = request.getHeader("X-Forwarded-For");
            if (forwarded == null || forwarded.isBlank()) {
                String address = request.getRemoteAddr();
                return address == null ? "unknown" : address;
            }
            int comma = forwarded.indexOf(',');
            return (comma < 0 ? forwarded : forwarded.substring(0, comma)).trim();
        };
    }

    /**
     * Keys by a header, typically an API key.
     *
     * <p>Requests without the header all share the {@code "anonymous"} bucket. That is intentional:
     * giving each of them its own key would mean omitting the header is a way around the limit.
     */
    static RateLimitKeyResolver byHeader(String headerName) {
        return request -> {
            String value = request.getHeader(headerName);
            return value == null || value.isBlank() ? "anonymous" : value;
        };
    }

    /**
     * Keys by the authenticated principal, falling back to the peer address for anonymous requests.
     */
    static RateLimitKeyResolver byPrincipal() {
        return request -> {
            java.security.Principal principal = request.getUserPrincipal();
            if (principal != null && principal.getName() != null) {
                return "user:" + principal.getName();
            }
            String address = request.getRemoteAddr();
            return "ip:" + (address == null ? "unknown" : address);
        };
    }
}
