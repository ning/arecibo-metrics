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
import com.yammer.metrics.reporting.JmxReporter.CounterMBean;
import com.yammer.metrics.reporting.JmxReporter.GaugeMBean;
import com.yammer.metrics.reporting.JmxReporter.HistogramMBean;
import com.yammer.metrics.reporting.JmxReporter.MeterMBean;
import com.yammer.metrics.reporting.JmxReporter.TimerMBean;
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
        TICK_THREAD.scheduleAtFixedRate(this, 0, period, unit);
    }

    @Override
    public void run() {
        for (Map.Entry<MetricName, Metric> entry : Metrics.allMetrics().entrySet()) {
            final MetricName name = entry.getKey();
            final Metric metric = entry.getValue();
    
            if (metric != null) {
                try {
                    final String mbeanName = name.getMBeanName();
    
                    if (metric instanceof GaugeMetric<?>) {
                        registerGauge((GaugeMetric<?>)metric, mbeanName);
                    } else if (metric instanceof CounterMetric) {
                        registerCounter((CounterMetric)metric, mbeanName);
                    } else if (metric instanceof HistogramMetric) {
                        registerHistogram((HistogramMetric)metric, mbeanName);
                    } else if (metric instanceof MeterMetric) {
                        registerMetered((MeterMetric)metric, mbeanName);
                    } else if (metric instanceof TimerMetric) {
                        registerTimer((TimerMetric)metric, mbeanName);
                    }
                }
                catch (Exception ignored) {
                }
            }
        }
    }

    private void registerValue(String mbeanName, String attributeName, Class<?> type) {
        profile.add(mbeanName,
                    attributeName,
                    attributeName,
                    Monitored.DEFAULT_EVENT_NAME_PATTERN,
                    Monitored.DEFAULT_EVENT_NAME,
                    new MonitoringType[] { MonitoringType.VALUE },
                    type);
    }

    private void registerCounter(String mbeanName, String attributeName, Class<?> type) {
        profile.add(mbeanName,
                    attributeName,
                    attributeName,
                    Monitored.DEFAULT_EVENT_NAME_PATTERN,
                    Monitored.DEFAULT_EVENT_NAME,
                    new MonitoringType[] { MonitoringType.COUNTER, MonitoringType.RATE },
                    type);
    }
    
    private void registerGauge(GaugeMetric<?> metric, String mbeanName) {
        profile.register(mbeanName, metric);
        registerValue(mbeanName, "value", GaugeMBean.class);
    }

    private void registerCounter(CounterMetric metric, String mbeanName) {
        profile.register(mbeanName, metric);
        registerCounter(mbeanName, "count", CounterMBean.class);
    }

    private void registerHistogram(HistogramMetric metric, String mbeanName) {
        profile.register(mbeanName, metric);
        registerCounter(mbeanName, "count", HistogramMBean.class);
        registerValue(mbeanName, "min", HistogramMBean.class);
        registerValue(mbeanName, "max", HistogramMBean.class);
        registerValue(mbeanName, "mean", HistogramMBean.class);
        registerValue(mbeanName, "stdDev", HistogramMBean.class);
        registerValue(mbeanName, "50thPercentile", HistogramMBean.class);
        registerValue(mbeanName, "75thPercentile", HistogramMBean.class);
        registerValue(mbeanName, "95thPercentile", HistogramMBean.class);
        registerValue(mbeanName, "98thPercentile", HistogramMBean.class);
        registerValue(mbeanName, "99thPercentile", HistogramMBean.class);
        registerValue(mbeanName, "999thPercentile", HistogramMBean.class);
    }

    private void registerMetered(MeterMetric metric, String mbeanName) {
        profile.register(mbeanName, metric);
        registerCounter(mbeanName, "count", MeterMBean.class);
        registerValue(mbeanName, "meanRate", MeterMBean.class);
        registerValue(mbeanName, "oneMinuteRate", MeterMBean.class);
        registerValue(mbeanName, "fiveMinuteRate", MeterMBean.class);
        registerValue(mbeanName, "fifteenMinuteRate", MeterMBean.class);
    }

    private void registerTimer(TimerMetric metric, String mbeanName) {
        profile.register(mbeanName, metric);
        registerCounter(mbeanName, "count", TimerMBean.class);
        registerValue(mbeanName, "min", TimerMBean.class);
        registerValue(mbeanName, "max", TimerMBean.class);
        registerValue(mbeanName, "mean", TimerMBean.class);
        registerValue(mbeanName, "stdDev", TimerMBean.class);
        registerValue(mbeanName, "50thPercentile", TimerMBean.class);
        registerValue(mbeanName, "75thPercentile", TimerMBean.class);
        registerValue(mbeanName, "95thPercentile", TimerMBean.class);
        registerValue(mbeanName, "98thPercentile", TimerMBean.class);
        registerValue(mbeanName, "99thPercentile", TimerMBean.class);
        registerValue(mbeanName, "999thPercentile", TimerMBean.class);
        registerValue(mbeanName, "meanRate", TimerMBean.class);
        registerValue(mbeanName, "oneMinuteRate", TimerMBean.class);
        registerValue(mbeanName, "fiveMinuteRate", TimerMBean.class);
        registerValue(mbeanName, "fifteenMinuteRate", TimerMBean.class);
    }
}
