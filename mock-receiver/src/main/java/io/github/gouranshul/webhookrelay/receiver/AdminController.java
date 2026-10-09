package io.github.gouranshul.webhookrelay.receiver;

import java.util.Map;
import java.util.TreeMap;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Test-only control plane: configure failures and read counters. Not something a real receiver exposes. */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final ReceiverState state;

    public AdminController(ReceiverState state) {
        this.state = state;
    }

    @PutMapping("/defaults")
    public Behavior setDefaults(@RequestBody Behavior behavior) {
        state.defaults(behavior);
        return behavior;
    }

    @GetMapping("/defaults")
    public Behavior getDefaults() {
        return state.defaults();
    }

    @PutMapping("/hooks/{name}")
    public Behavior configure(@PathVariable String name, @RequestBody Behavior behavior) {
        state.configure(name, behavior);
        return behavior;
    }

    @GetMapping("/hooks/{name}/stats")
    public HookState.Stats stats(@PathVariable String name) {
        return state.hook(name).stats();
    }

    @DeleteMapping("/hooks/{name}")
    public ResponseEntity<Void> reset(@PathVariable String name) {
        state.reset(name);
        return ResponseEntity.noContent().build();
    }

    /** Totals across all hooks plus per-hook stats, for the load test summary. */
    @GetMapping("/stats")
    public Map<String, Object> all() {
        Map<String, HookState.Stats> perHook = new TreeMap<>();
        long requests = 0;
        long unique = 0;
        long duplicates = 0;
        long signatureFailures = 0;
        for (var entry : state.all().entrySet()) {
            HookState.Stats s = entry.getValue().stats();
            perHook.put(entry.getKey(), s);
            requests += s.requests();
            unique += s.uniqueSucceeded();
            duplicates += s.duplicates();
            signatureFailures += s.signatureFailures();
        }
        return Map.of("requests", requests, "uniqueSucceeded", unique, "duplicates", duplicates,
                "signatureFailures", signatureFailures, "hooks", perHook);
    }
}
