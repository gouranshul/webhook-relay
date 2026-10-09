package io.github.gouranshul.webhookrelay.dispatcher.http;

import io.github.gouranshul.webhookrelay.common.net.EndpointUrlPolicy;
import io.github.gouranshul.webhookrelay.common.net.EndpointUrlPolicy.InvalidEndpointUrlException;
import io.github.gouranshul.webhookrelay.common.signing.WebhookHeaders;
import io.github.gouranshul.webhookrelay.common.signing.WebhookSigner;
import io.github.gouranshul.webhookrelay.dispatcher.config.DispatcherProperties;
import io.github.gouranshul.webhookrelay.dispatcher.endpoint.EndpointConfig;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Sends one signed webhook request with the JDK {@link HttpClient}. Never follows redirects (a redirect could
 * bounce the request to an internal address), always has a connect and a request timeout, and reads at most a
 * few hundred bytes of an error body. Thread-safe; one instance is shared.
 */
public class WebhookSender {

    public static final String USER_AGENT = "webhook-relay/0.1";

    private final HttpClient client;
    private final WebhookSigner signer = new WebhookSigner();
    private final EndpointUrlPolicy urlPolicy;
    private final Duration requestTimeout;
    private final int errorBodyLimit;
    private final Clock clock;

    /** @param executor for the client's internal async work; null for the JDK default. Never a bounded pool shared with callers. */
    public WebhookSender(DispatcherProperties.Dispatcher config, EndpointUrlPolicy urlPolicy, Executor executor,
            Clock clock) {
        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(config.connectTimeout())
                .followRedirects(HttpClient.Redirect.NEVER)
                .version(HttpClient.Version.HTTP_1_1);
        if (executor != null) {
            builder.executor(executor);
        }
        this.client = builder.build();
        this.urlPolicy = urlPolicy;
        this.requestTimeout = config.requestTimeout();
        this.errorBodyLimit = config.errorBodyLimit();
        this.clock = clock;
    }

    /**
     * @param extraHeaders e.g. W3C {@code traceparent}; may be empty
     */
    public HttpOutcome send(EndpointConfig endpoint, String webhookId, byte[] body, Map<String, String> extraHeaders) {
        long started = System.nanoTime();
        URI uri;
        try {
            // Re-checked on every send: DNS may now resolve to an internal address (rebinding).
            uri = urlPolicy.validate(endpoint.url());
        } catch (InvalidEndpointUrlException e) {
            return new HttpOutcome(HttpOutcome.Kind.PERMANENT, null, elapsed(started), null, e.getMessage());
        }
        long timestamp = clock.instant().getEpochSecond();
        HttpRequest.Builder request = HttpRequest.newBuilder(uri)
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                .header("User-Agent", USER_AGENT)
                .header(WebhookHeaders.ID, webhookId)
                .header(WebhookHeaders.TIMESTAMP, Long.toString(timestamp))
                .header(WebhookHeaders.SIGNATURE, signer.sign(endpoint.secret(), timestamp, body))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        extraHeaders.forEach(request::header);

        try {
            HttpResponse<InputStream> response = client.send(request.build(), HttpResponse.BodyHandlers.ofInputStream());
            int status = response.statusCode();
            HttpOutcome.Kind kind = HttpOutcome.classify(status);
            String error = null;
            try (InputStream in = response.body()) {
                if (kind != HttpOutcome.Kind.SUCCESS) {
                    error = "HTTP " + status + ": " + new String(in.readNBytes(errorBodyLimit), StandardCharsets.UTF_8);
                } else {
                    in.skip(Long.MAX_VALUE);
                }
            } catch (IOException bodyError) {
                // The status is what matters; a body that fails mid-read does not change the outcome.
            }
            Duration retryAfter = kind.retryable()
                    ? RetryAfter.parse(response.headers().firstValue("Retry-After").orElse(null), clock)
                    : null;
            return new HttpOutcome(kind, status, elapsed(started), retryAfter, truncate(error));
        } catch (HttpConnectTimeoutException e) {
            return new HttpOutcome(HttpOutcome.Kind.CONNECTION_ERROR, null, elapsed(started), null, "connect timeout");
        } catch (HttpTimeoutException e) {
            return new HttpOutcome(HttpOutcome.Kind.TIMEOUT, null, elapsed(started), null,
                    "no response within " + requestTimeout.toMillis() + " ms");
        } catch (ConnectException e) {
            return new HttpOutcome(HttpOutcome.Kind.CONNECTION_ERROR, null, elapsed(started), null,
                    truncate("connection failed: " + e.getMessage()));
        } catch (IOException e) {
            return new HttpOutcome(HttpOutcome.Kind.CONNECTION_ERROR, null, elapsed(started), null,
                    truncate("I/O error: " + e));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new HttpOutcome(HttpOutcome.Kind.CONNECTION_ERROR, null, elapsed(started), null, "interrupted");
        }
    }

    private String truncate(String s) {
        if (s == null || s.length() <= errorBodyLimit) {
            return s;
        }
        return s.substring(0, errorBodyLimit) + "...";
    }

    private static Duration elapsed(long startedNanos) {
        return Duration.ofNanos(System.nanoTime() - startedNanos);
    }
}
