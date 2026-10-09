package io.github.gouranshul.webhookrelay.api.endpoint;

import io.github.gouranshul.webhookrelay.api.endpoint.EndpointDtos.CreateEndpointRequest;
import io.github.gouranshul.webhookrelay.api.endpoint.EndpointDtos.CreatedEndpoint;
import io.github.gouranshul.webhookrelay.api.endpoint.EndpointDtos.UpdateEndpointRequest;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/endpoints")
public class EndpointController {

    private final EndpointService service;

    public EndpointController(EndpointService service) {
        this.service = service;
    }

    /** Registers an endpoint. The response is the only time the signing secret is ever shown. */
    @PostMapping
    public ResponseEntity<CreatedEndpoint> create(@Valid @RequestBody CreateEndpointRequest request) {
        CreatedEndpoint created = service.create(request);
        return ResponseEntity.created(URI.create("/v1/endpoints/" + created.endpoint().id())).body(created);
    }

    @GetMapping
    public List<Endpoint> list(@RequestParam(defaultValue = "100") int limit) {
        return service.list(limit);
    }

    @GetMapping("/{id}")
    public Endpoint get(@PathVariable UUID id) {
        return service.get(id);
    }

    @PatchMapping("/{id}")
    public Endpoint update(@PathVariable UUID id, @Valid @RequestBody UpdateEndpointRequest request) {
        return service.update(id, request);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable UUID id) {
        service.delete(id);
        return ResponseEntity.noContent().build();
    }
}
