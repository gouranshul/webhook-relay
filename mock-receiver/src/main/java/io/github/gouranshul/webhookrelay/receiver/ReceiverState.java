package io.github.gouranshul.webhookrelay.receiver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/** All hooks, keyed by name. The number of hooks is capped so a misbehaving client cannot exhaust memory. */
@Component
public class ReceiverState {

    private final ConcurrentHashMap<String, HookState> hooks = new ConcurrentHashMap<>();
    private final ReceiverProperties properties;
    private volatile Behavior defaults;

    public ReceiverState(ReceiverProperties properties) {
        this.properties = properties;
        this.defaults = properties.defaultBehavior();
    }

    public HookState hook(String name) {
        HookState existing = hooks.get(name);
        if (existing != null) {
            return existing;
        }
        if (hooks.size() >= properties.maxHooks()) {
            throw new IllegalStateException("too many hooks");
        }
        return hooks.computeIfAbsent(name, n -> new HookState(null, properties.maxRecordedPerHook()));
    }

    /** Effective behaviour: the hook's override, else the defaults. */
    public Behavior behaviorOf(HookState hook) {
        Behavior own = hook.behavior();
        return own != null ? own : defaults;
    }

    public void configure(String name, Behavior behavior) {
        hook(name).behavior(behavior);
    }

    public Behavior defaults() {
        return defaults;
    }

    public void defaults(Behavior behavior) {
        this.defaults = behavior;
    }

    public void reset(String name) {
        hooks.remove(name);
    }

    public Map<String, HookState> all() {
        return Map.copyOf(hooks);
    }
}
