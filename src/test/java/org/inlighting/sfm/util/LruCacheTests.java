package org.inlighting.sfm.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class LruCacheTests {

    @Test
    void lruCacheTest() {
        LruCache<Integer, String> lru = new LruCache<>(2);
        lru.put(1, "1");
        lru.put(2, "2");
        lru.put(3, "3");

        assertNull(lru.get(1));

        lru.get(2);
        lru.put(4, "4");
        assertNull(lru.get(3));
        assertEquals("2", lru.get(2));
        assertEquals("4", lru.get(4));
    }
}
