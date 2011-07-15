package com.ning.arecibo.metrics;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ning.arecibo.jmx.AreciboProfile;
import com.ning.arecibo.jmx.Monitored;
import com.ning.arecibo.jmx.MonitoringType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.CounterMetric;
import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.core.HistogramMetric;
import com.yammer.metrics.core.MeterMetric;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.TimerMetric;
import com.yammer.metrics.util.Utils;

public class AreciboMetricsReporter implements Runnable {
    private static final ScheduledExecutorService TICK_THREAD = Utils.newScheduledThreadPool(1, "arecibo-reporter");

    private final AreciboProfile profile;

    /**
     * Enables the arecibo reporter.
     * 
     * @param profile The arecibo profile to keep up to date
     */
    public static AreciboMetricsReporter enable(AreciboProfile profile) {
        final AreciboMetricsReporter reporter = new AreciboMetricsReporter(profile);

        reporter.start(1, TimeUnit.MINUTES);
        return reporter;
    }

    /**
     * Creates a new {@link AreciboMetricsReporter}.
     * 
     * @param profile The arecibo profile to keep up to date
     */
    public AreciboMetricsReporter(AreciboProfile profile) {
        this.profile = profile;
    }

    /**
     * Starts this reporter.
     *
     * @param period the period between successive checks
     * @param unit   the time unit of {@code period}
     */
    public void start(long period, TimeUnit unit) {
        TICK_THREAD.scheduleAtFixedRate(this, period, period, unit);
    }

    @Override
    public void run() {
        for (Map.Entry<MetricName, Metric> entry : Metrics.allMetrics().entrySet()) {
            final MetricName name = entry.getKey();
            final Metric metric = entry.getValue();
    
            if (metric != null) {
                final String mbeanName = name.getMBeanName();

                if (metric instanceof GaugeMetric<?>) {
                    registerGauge(mbeanName);
                } else if (metric instanceof CounterMetric) {
                    registerCounter(mbeanName);
                } else if (metric instanceof HistogramMetric) {
                    registerHistogram(mbeanName);
                } else if (metric instanceof MeterMetric) {
                    registerMetered(mbeanName);
                } else if (metric instanceof TimerMetric) {
                    registerTimer(mbeanName);
                }
            }
        }
    }

    private void registerValue(String mbeanName, String attributeName) {
        profile.add(mbeanName,
                    attributeName,
                    attributeName,
                    Monitored.DEFAULT_EVENT_NAME_PATTERN,
                    Monitored.DEFAULT_EVENT_NAME,
                    new MonitoringType[] { MonitoringType.VALUE },
                    null);
    }

    private void registerCounter(String mbeanName, String attributeName) {
        profile.add(mbeanName,
                    attributeName,
                    attributeName,
                    Monitored.DEFAULT_EVENT_NAME_PATTERN,
                    Monitored.DEFAULT_EVENT_NAME,
                    new MonitoringType[] { MonitoringType.COUNTER, MonitoringType.RATE },
                    null);
    }
    
    private void registerGauge(String mbeanName) {
        registerValue(mbeanName, "value");
    }

    private void registerCounter(String mbeanName) {
        registerCounter(mbeanName, "count");
    }

    private void registerHistogram(String mbeanName) {
        registerCounter(mbeanName, "count");
        registerValue(mbeanName, "min");
        registerValue(mbeanName, "max");
        registerValue(mbeanName, "mean");
        registerValue(mbeanName, "stdDev");
        registerValue(mbeanName, "50thPercentile");
        registerValue(mbeanName, "75thPercentile");
        registerValue(mbeanName, "95thPercentile");
        registerValue(mbeanName, "98thPercentile");
        registerValue(mbeanName, "99thPercentile");
        registerValue(mbeanName, "999thPercentile");
    }

    private void registerMetered(String mbeanName) {
        registerCounter(mbeanName, "count");
        registerValue(mbeanName, "meanRate");
        registerValue(mbeanName, "oneMinuteRate");
        registerValue(mbeanName, "fiveMinuteRate");
        registerValue(mbeanName, "fifteenMinuteRate");
    }

    private void registerTimer(String mbeanName) {
        registerMetered(mbeanName);
        registerHistogram(mbeanName);
    }
}
