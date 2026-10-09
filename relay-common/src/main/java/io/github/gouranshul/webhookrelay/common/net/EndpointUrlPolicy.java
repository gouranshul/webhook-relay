package io.github.gouranshul.webhookrelay.common.net;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Locale;

/**
 * Guards against server-side request forgery: a webhook URL is user input, and without checks the relay would
 * happily POST to {@code http://169.254.169.254/} or an internal admin port.
 *
 * <p>Checked at registration and again before every send (DNS can change between the two). Private networks
 * can be allowed explicitly for local development, where the mock receiver lives on a private address.
 */
public final class EndpointUrlPolicy {

    private final boolean allowPrivateNetworks;

    public EndpointUrlPolicy(boolean allowPrivateNetworks) {
        this.allowPrivateNetworks = allowPrivateNetworks;
    }

    /** Validates syntax and, unless private networks are allowed, that the host resolves to public addresses. */
    public URI validate(String url) {
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException | NullPointerException e) {
            throw new InvalidEndpointUrlException("URL is not valid: " + url);
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!scheme.equals("https") && !scheme.equals("http")) {
            throw new InvalidEndpointUrlException("URL scheme must be http or https");
        }
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            throw new InvalidEndpointUrlException("URL must have a host");
        }
        if (uri.getRawUserInfo() != null) {
            throw new InvalidEndpointUrlException("URL must not contain credentials");
        }
        if (uri.getRawFragment() != null) {
            throw new InvalidEndpointUrlException("URL must not contain a fragment");
        }
        if (!allowPrivateNetworks) {
            checkResolvesPublic(uri.getHost());
        }
        return uri;
    }

    private static void checkResolvesPublic(String host) {
        InetAddress[] addresses;
        try {
            addresses = InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            throw new InvalidEndpointUrlException("URL host does not resolve: " + host);
        }
        for (InetAddress address : addresses) {
            if (isNonPublic(address)) {
                throw new InvalidEndpointUrlException("URL resolves to a non-public address: " + host);
            }
        }
    }

    static boolean isNonPublic(InetAddress address) {
        if (address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isLinkLocalAddress()
                || address.isSiteLocalAddress() || address.isMulticastAddress()) {
            return true;
        }
        byte[] b = address.getAddress();
        if (address instanceof Inet4Address) {
            int first = b[0] & 0xFF;
            int second = b[1] & 0xFF;
            return first == 0                                    // "this" network
                    || (first == 100 && second >= 64 && second <= 127) // carrier-grade NAT 100.64.0.0/10
                    || (first == 192 && second == 0 && (b[2] & 0xFF) == 0) // IETF protocol assignments
                    || (first == 198 && (second == 18 || second == 19))  // benchmarking 198.18.0.0/15
                    || first >= 240;                             // reserved and broadcast
        }
        if (address instanceof Inet6Address) {
            return (b[0] & 0xFE) == 0xFC; // unique local fc00::/7
        }
        return false;
    }

    public static final class InvalidEndpointUrlException extends IllegalArgumentException {
        public InvalidEndpointUrlException(String message) {
            super(message);
        }
    }
}
