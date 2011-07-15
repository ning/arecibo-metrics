package com.ning.arecibo.metrics.guice;

import com.google.inject.AbstractModule;
import com.ning.arecibo.metrics.AreciboMetricsReporter;

public class AreciboMetricsModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(AreciboMetricsReporter.class).toProvider(AreciboMetricsReporterProvider.class).asEagerSingleton();
    }
}
