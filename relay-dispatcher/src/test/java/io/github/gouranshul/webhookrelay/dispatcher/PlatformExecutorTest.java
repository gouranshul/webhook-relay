package io.github.gouranshul.webhookrelay.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gouranshul.webhookrelay.receiver.Behavior;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

/**
 * Regression test for a starvation deadlock found in a load test: with a fixed platform pool that was also the
 * HttpClient's executor, once every pool thread blocked in send() no thread remained to complete responses.
 * With a 2-thread pool and more concurrent attempts than threads, every delivery must still succeed.
 */
@TestPropertySource(properties = {
    "relay.dispatcher.executor=platform",
    "relay.dispatcher.platform-pool-size=2",
    "relay.dispatcher.request-timeout=5s"
})
class PlatformExecutorTest extends DispatcherIntegrationTest {

    @Test
    void boundedPlatformPoolDoesNotStarveTheHttpClient() {
        TestEndpoint endpoint = endpoint(new Behavior(0, 300, 300, 503, null, null, null));
        for (int i = 0; i < 10; i++) {
            enqueue(endpoint);
        }
        await().atMost(Duration.ofSeconds(30)).until(() -> hook(endpoint).stats().uniqueSucceeded() == 10);
        assertThat(jdbc.sql("SELECT count(*) FROM delivery_attempts a JOIN deliveries d ON d.id = a.delivery_id WHERE d.endpoint_id = ? AND a.outcome <> 'SUCCESS'")
                .param(endpoint.id()).query(Long.class).single()).isZero();
    }
}
