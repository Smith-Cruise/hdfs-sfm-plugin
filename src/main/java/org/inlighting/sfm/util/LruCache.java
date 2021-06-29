package org.inlighting.sfm.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class LruCache<K, V> extends LinkedHashMap<K, V> {
    private final int MAX_ENTRIES;

    public LruCache(int maxEntries) {
        super(maxEntries + 1, 1.0f, true);
        MAX_ENTRIES = maxEntries;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > MAX_ENTRIES;
    }

    @Override
    public V put(K key, V value) {
        return super.put(key, value);
    }

    @Override
    public V get(Object key) {
        V value = super.get(key);
        if (value == null) {
            return null;
        } else {
            super.remove(key);
            super.put((K)key, value);
            return value;
        }
    }
}
