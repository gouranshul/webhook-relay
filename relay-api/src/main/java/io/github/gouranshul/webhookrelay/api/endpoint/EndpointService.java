package io.github.gouranshul.webhookrelay.api.endpoint;

import io.github.gouranshul.webhookrelay.api.config.RelayApiProperties;
import io.github.gouranshul.webhookrelay.api.endpoint.EndpointDtos.CreateEndpointRequest;
import io.github.gouranshul.webhookrelay.api.endpoint.EndpointDtos.CreatedEndpoint;
import io.github.gouranshul.webhookrelay.api.endpoint.EndpointDtos.UpdateEndpointRequest;
import io.github.gouranshul.webhookrelay.api.web.ApiException;
import io.github.gouranshul.webhookrelay.common.crypto.SecretCipher;
import io.github.gouranshul.webhookrelay.common.crypto.SigningSecrets;
import io.github.gouranshul.webhookrelay.common.model.EndpointStatus;
import io.github.gouranshul.webhookrelay.common.net.EndpointUrlPolicy;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class EndpointService {

    private static final Logger log = LoggerFactory.getLogger(EndpointService.class);

    private final EndpointRepository repository;
    private final SecretCipher cipher;
    private final EndpointUrlPolicy urlPolicy;
    private final RelayApiProperties properties;

    public EndpointService(EndpointRepository repository, SecretCipher cipher, EndpointUrlPolicy urlPolicy,
            RelayApiProperties properties) {
        this.repository = repository;
        this.cipher = cipher;
        this.urlPolicy = urlPolicy;
        this.properties = properties;
    }

    @Transactional
    public CreatedEndpoint create(CreateEndpointRequest request) {
        urlPolicy.validate(request.url());
        String secret = SigningSecrets.generate();
        UUID secretId = UUID.randomUUID();
        repository.insertSecret(secretId, cipher.keyId(), cipher.encrypt(secretId, secret));
        int rateLimit = request.rateLimitPerSec() != null
                ? request.rateLimitPerSec()
                : properties.endpoints().defaultRateLimitPerSec();
        Endpoint endpoint = repository.insert(UUID.randomUUID(), request.url(), request.description(),
                distinct(request.eventTypes()), rateLimit, secretId);
        return new CreatedEndpoint(endpoint, secret);
    }

    @Transactional(readOnly = true)
    public Endpoint get(UUID id) {
        return repository.find(id).orElseThrow(() -> ApiException.notFound("Endpoint", id));
    }

    @Transactional(readOnly = true)
    public List<Endpoint> list(int limit) {
        return repository.list(Math.clamp(limit, 1, 500));
    }

    @Transactional
    public Endpoint update(UUID id, UpdateEndpointRequest request) {
        Endpoint current = repository.findForUpdate(id).orElseThrow(() -> ApiException.notFound("Endpoint", id));
        if (request.status() == EndpointStatus.DELETED) {
            throw ApiException.badRequest("Use DELETE to remove an endpoint");
        }
        if (request.url() != null) {
            urlPolicy.validate(request.url());
        }
        EndpointStatus status = request.status() != null ? request.status() : current.status();
        boolean resuming = current.status() == EndpointStatus.PAUSED && status == EndpointStatus.ACTIVE;
        Endpoint updated = repository.update(id,
                request.url() != null ? request.url() : current.url(),
                request.description() != null ? request.description() : current.description(),
                request.eventTypes() != null ? distinct(request.eventTypes()) : current.eventTypes(),
                request.rateLimitPerSec() != null ? request.rateLimitPerSec() : current.rateLimitPerSec(),
                status,
                resuming);
        if (resuming) {
            int requeued = repository.unparkDeliveries(id);
            log.info("Endpoint {} resumed; re-enqueued {} parked deliveries", id, requeued);
        }
        return updated;
    }

    @Transactional
    public void delete(UUID id) {
        if (!repository.softDelete(id)) {
            throw ApiException.notFound("Endpoint", id);
        }
    }

    private static List<String> distinct(List<String> types) {
        return types.stream().distinct().toList();
    }
}
