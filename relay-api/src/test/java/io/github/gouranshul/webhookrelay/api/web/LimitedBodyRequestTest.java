package io.github.gouranshul.webhookrelay.api.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

/** Covers bodies without a Content-Length (chunked), which the up-front check cannot see. */
class LimitedBodyRequestTest {

    private static MockHttpServletRequest requestWithBody(int bytes) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/v1/events");
        request.setContent(new byte[bytes]);
        return request;
    }

    @Test
    void allowsBodiesUpToTheLimit() throws Exception {
        try (InputStream in = new LimitedBodyRequest(requestWithBody(100), 100).getInputStream()) {
            assertThat(in.readAllBytes()).hasSize(100);
        }
    }

    @Test
    void failsOnceTheLimitIsExceeded() {
        assertThatThrownBy(() -> {
            try (InputStream in = new LimitedBodyRequest(requestWithBody(101), 100).getInputStream()) {
                in.readAllBytes();
            }
        }).isInstanceOf(LimitedBodyRequest.BodyTooLargeException.class).hasMessageContaining("100 bytes");
    }

    @Test
    void countsSingleByteReadsToo() throws Exception {
        try (InputStream in = new LimitedBodyRequest(requestWithBody(3), 2).getInputStream()) {
            in.read();
            in.read();
            assertThatThrownBy(in::read).isInstanceOf(LimitedBodyRequest.BodyTooLargeException.class);
        }
    }
}
