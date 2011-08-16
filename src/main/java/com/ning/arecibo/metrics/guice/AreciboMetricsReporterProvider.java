package com.ning.arecibo.metrics.guice;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.ning.arecibo.jmx.AreciboProfile;
import com.ning.arecibo.metrics.AreciboMetricsReporter;
import com.yammer.metrics.core.MetricsRegistry;

public class AreciboMetricsReporterProvider implements Provider<AreciboMetricsReporter>
{
    private final MetricsRegistry metricsRegistry;
    private final AreciboProfile profile;

    @Inject
    public AreciboMetricsReporterProvider(MetricsRegistry metricsRegistry, AreciboProfile profile)
    {
        this.metricsRegistry = metricsRegistry;
        this.profile = profile;
    }

    @Override
    public AreciboMetricsReporter get()
    {
        return AreciboMetricsReporter.enable(metricsRegistry, profile);
    }
}
