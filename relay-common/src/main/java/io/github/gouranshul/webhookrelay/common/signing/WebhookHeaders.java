package io.github.gouranshul.webhookrelay.common.signing;

/** Header names sent with every webhook request. */
public final class WebhookHeaders {

    /** Stable message id. Identical across retries and replays, so receivers can dedupe on it. */
    public static final String ID = "Webhook-Id";

    /** Unix epoch seconds at signing time. Part of the signed content to prevent replays. */
    public static final String TIMESTAMP = "Webhook-Timestamp";

    /** Space-separated list of {@code v1=<hex>} signatures. More than one appears during secret rotation. */
    public static final String SIGNATURE = "Webhook-Signature";

    private WebhookHeaders() {
    }
}
