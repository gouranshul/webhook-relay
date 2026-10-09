package io.github.gouranshul.webhookrelay.api.web;

import io.github.gouranshul.webhookrelay.api.config.RelayApiProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Requires {@code Authorization: Bearer <token>} on {@code /v1/**}. Also rejects oversized bodies up front,
 * before Jackson buffers them.
 */
@Component
public class ApiTokenFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(ApiTokenFilter.class);
    private static final String PREFIX = "Bearer ";

    private final byte[] token;
    private final long maxBodyBytes;

    public ApiTokenFilter(RelayApiProperties properties) {
        String configured = properties.api().token();
        this.token = configured == null || configured.isBlank() ? null : configured.getBytes(StandardCharsets.UTF_8);
        // The envelope around an event's data is small; allow a little headroom over the payload limit.
        this.maxBodyBytes = properties.events().maxPayloadBytes() + 4096L;
        if (token == null) {
            log.warn("relay.api.token is not set: the management API is UNAUTHENTICATED. Use only for local development.");
        }
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/v1/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (token != null && !authorized(request.getHeader(HttpHeaders.AUTHORIZATION))) {
            problem(response, HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized", "Missing or invalid bearer token");
            return;
        }
        if (request.getContentLengthLong() > maxBodyBytes) {
            problem(response, HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE, "Payload Too Large",
                    "Request body exceeds " + maxBodyBytes + " bytes");
            return;
        }
        chain.doFilter(request, response);
    }

    private boolean authorized(String header) {
        if (header == null || !header.startsWith(PREFIX)) {
            return false;
        }
        byte[] provided = header.substring(PREFIX.length()).trim().getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(token, provided);
    }

    private static void problem(HttpServletResponse response, int status, String title, String detail)
            throws IOException {
        response.setStatus(status);
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        if (status == HttpServletResponse.SC_UNAUTHORIZED) {
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer");
        }
        response.getWriter().write("{\"title\":\"" + title + "\",\"status\":" + status + ",\"detail\":\"" + detail + "\"}");
    }
}
