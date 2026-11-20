package tu.kaninchen.mq.broker.core.exchange;

import tu.kaninchen.mq.broker.core.queue.Queue;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Exchange {

    private final String name;
    private final ExchangeType type;
    private final Set<Binding> bindings;

    public Exchange(String name, ExchangeType type) {
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
        this.bindings = new HashSet<>();
    }

    public boolean addBinding(Binding binding) {
        return bindings.add(binding);
    }

    public boolean removeBinding(Binding binding) {
        return bindings.remove(binding);
    }

    public Set<Queue> getMatchingQueues(String routingKey) {
        return switch (this.type) {
            case DIRECT -> matchDirect(routingKey);
            case TOPIC -> matchTopic(routingKey);
            case FANOUT -> matchFanout();
        };
    }

    private Set<Queue> matchDirect(String routingKey) {
        return bindings.stream()
            .filter(binding -> binding.bindingKey().equals(routingKey))
            .map(Binding::queue)
            .collect(Collectors.toSet());
    }

    private Set<Queue> matchFanout() {
        return bindings.stream()
            .map(Binding::queue)
            .collect(Collectors.toSet());
    }

    private Set<Queue> matchTopic(String routingKey) {
        String[] routingSegments = routingKey.split("\\.");
        return bindings.stream()
            .filter(binding -> {
                String[] bindingSegments = binding.bindingKey().split("\\.");
                return matches(bindingSegments, routingSegments, 0, 0);
            })
            .map(Binding::queue)
            .collect(Collectors.toSet());
    }

    private boolean matches(String[] binding, String[] routing, int bindingIndex, int routingIndex) {
        if (bindingIndex == binding.length && routingIndex == routing.length) {
            return true;
        }
        if (bindingIndex == binding.length - 1 && binding[bindingIndex].equals("#")) {
            return true;
        }
        if (bindingIndex < binding.length && routingIndex < routing.length) {
            String bindingSegment = binding[bindingIndex];
            String routingSegment = routing[routingIndex];
            if (bindingSegment.equals(routingSegment) || bindingSegment.equals("*")) {
                return matches(binding, routing, bindingIndex + 1, routingIndex + 1);
            }
            if (bindingSegment.equals("#")) {
                return matches(binding, routing, bindingIndex + 1, routingIndex)
                    || matches(binding, routing, bindingIndex, routingIndex + 1);
            }
        }
        return false;
    }

    public int getBindingCount() {
        return bindings.size();
    }

    public String getName() {
        return name;
    }

    public ExchangeType getType() {
        return type;
    }
}
