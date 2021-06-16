package org.inlighting.sfm.util;

import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Tracer;

import java.util.HashMap;
import java.util.Map;

public class SFMTracer {
    private static Tracer instance;

    public static synchronized Tracer get() {
        if (instance == null) {
            Map<String, String> config = new HashMap<>();
            config.put("sampler.classes", "org.apache.htrace.core.AlwaysSampler");
//            config.put("sampler.classes", "org.apache.htrace.core.NeverSampler");
            config.put("span.receiver.classes", "org.apache.htrace.impl.ZipkinSpanReceiver");
            config.put("zipkin.scribe.hostname", "monitor.lab.com");
            config.put("zipkin.scribe.port", "9410");
            instance = new Tracer.Builder("SFMClient").conf(HTraceConfiguration.fromMap(config)).build();
        }
        return instance;
    }

}
