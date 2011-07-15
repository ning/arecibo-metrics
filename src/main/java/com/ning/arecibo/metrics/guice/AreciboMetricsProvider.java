package com.ning.arecibo.metrics.guice;

import javax.xml.ws.Provider;

import com.google.inject.Inject;
import com.ning.arecibo.jmx.AreciboProfile;
import com.ning.arecibo.metrics.AreciboMetricsReporter;

public class AreciboMetricsProvider implements Provider<AreciboMetricsReporter>
{
    private final AreciboProfile profile;

    @Inject
    public AreciboMetricsProvider(AreciboProfile profile)
    {
        this.profile = profile;
    }

    @Override
    public AreciboMetricsReporter invoke(AreciboMetricsReporter request)
    {
        return AreciboMetricsReporter.enable(profile);
    }
}
