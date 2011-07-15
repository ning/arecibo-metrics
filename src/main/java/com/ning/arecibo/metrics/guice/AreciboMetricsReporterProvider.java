package com.ning.arecibo.metrics.guice;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.ning.arecibo.jmx.AreciboProfile;
import com.ning.arecibo.metrics.AreciboMetricsReporter;

public class AreciboMetricsReporterProvider implements Provider<AreciboMetricsReporter>
{
    private final AreciboProfile profile;

    @Inject
    public AreciboMetricsReporterProvider(AreciboProfile profile)
    {
        this.profile = profile;
    }

    @Override
    public AreciboMetricsReporter get()
    {
        return AreciboMetricsReporter.enable(profile);
    }
}
