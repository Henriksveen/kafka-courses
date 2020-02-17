package no.safebase.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Counter<T> {
    final Map<T, Integer> counts = new HashMap<>();

    public void add(T t) {
        counts.merge(t, 1, Integer::sum);
    }

    public int count(T t) {
        return counts.getOrDefault(t, 0);
    }

    public void resetAll() {
        counts.clear();
    }

    public Set<T> getKeys() {
        return counts.keySet();
    }

    public int totalSum() {
        return counts.values().stream().mapToInt(Integer::intValue).sum();
    }

    @Override
    public String toString() {
        return "Counter{" +
                counts.toString().replaceAll(", ", ",\n") +
                '}';
    }
}
