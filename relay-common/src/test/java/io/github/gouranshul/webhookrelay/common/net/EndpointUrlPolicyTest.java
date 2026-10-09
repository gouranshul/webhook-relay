package io.github.gouranshul.webhookrelay.common.net;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.gouranshul.webhookrelay.common.net.EndpointUrlPolicy.InvalidEndpointUrlException;
import java.net.InetAddress;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

class EndpointUrlPolicyTest {

    private final EndpointUrlPolicy strict = new EndpointUrlPolicy(false);
    private final EndpointUrlPolicy dev = new EndpointUrlPolicy(true);

    @ParameterizedTest
    @ValueSource(strings = {
        "http://127.0.0.1/hook", "http://localhost:8080/", "http://10.1.2.3/", "http://192.168.0.10/",
        "http://172.16.5.5/", "http://169.254.169.254/latest/meta-data", "http://[::1]/", "http://0.0.0.0/",
        "http://100.64.0.1/", "http://[fd00::1]/"
    })
    void rejectsNonPublicTargetsByDefault(String url) {
        assertThatThrownBy(() -> strict.validate(url)).isInstanceOf(InvalidEndpointUrlException.class);
    }

    @Test
    void allowsPrivateTargetsWhenConfigured() {
        assertThat(dev.validate("http://mock-receiver:8090/hooks/a").getHost()).isEqualTo("mock-receiver");
        assertThat(dev.validate("http://127.0.0.1:9000/x").getPort()).isEqualTo(9000);
    }

    @ParameterizedTest
    @ValueSource(strings = {"ftp://example.com/", "file:///etc/passwd", "not a url", "http:///nohost",
        "https://user:pass@example.com/", "https://example.com/#frag", "gopher://example.com"})
    void rejectsBadSyntaxAndSchemes(String url) {
        assertThatThrownBy(() -> dev.validate(url)).isInstanceOf(InvalidEndpointUrlException.class);
    }

    @Test
    void classifiesPublicAddresses() throws Exception {
        assertThat(EndpointUrlPolicy.isNonPublic(InetAddress.getByName("93.184.215.14"))).isFalse();
        assertThat(EndpointUrlPolicy.isNonPublic(InetAddress.getByName("2606:4700::1111"))).isFalse();
        assertThat(EndpointUrlPolicy.isNonPublic(InetAddress.getByName("198.18.0.1"))).isTrue();
        assertThat(EndpointUrlPolicy.isNonPublic(InetAddress.getByName("255.255.255.255"))).isTrue();
    }
}
