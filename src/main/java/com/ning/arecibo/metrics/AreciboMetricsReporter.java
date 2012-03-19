package com.ning.arecibo.metrics;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ning.arecibo.jmx.AreciboMBeanExporter;
import com.ning.arecibo.jmx.AreciboProfile;
import com.ning.arecibo.jmx.Monitored;
import com.ning.arecibo.jmx.MonitoringType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.JmxReporter.CounterMBean;
import com.yammer.metrics.reporting.JmxReporter.GaugeMBean;
import com.yammer.metrics.reporting.JmxReporter.HistogramMBean;
import com.yammer.metrics.reporting.JmxReporter.MeterMBean;
import com.yammer.metrics.reporting.JmxReporter.TimerMBean;

public class AreciboMetricsReporter implements Runnable {
    private final ScheduledExecutorService tickThread;
    private final MetricsRegistry metricsRegistry;
    private final AreciboProfile profile;
    private final AreciboMBeanExporter mbeanExporter;

    /**
     * Enables the arecibo reporter. Note that this method is not thread safe.
     *
     * @param profile the arecibo profile to keep up to date
     */
    public static AreciboMetricsReporter enable(AreciboProfile profile) {
        return enable(Metrics.defaultRegistry(), profile);
    }

    /**
     * Enables the arecibo reporter. Note that this method is not thread safe.
     *
     * @param metricsRegistry the metrics registry
     * @param profile the arecibo profile to keep up to date
     */
    public static AreciboMetricsReporter enable(MetricsRegistry metricsRegistry, AreciboProfile profile) {
        final AreciboMetricsReporter reporter = new AreciboMetricsReporter(metricsRegistry, profile);

        reporter.start(1, TimeUnit.MINUTES);
        return reporter;
    }

    /**
     * Creates a new {@link AreciboMetricsReporter}.
     * 
     * @param metricsRegistry the metrics registry
     * @param profile the arecibo profile to keep up to date
     */
    public AreciboMetricsReporter(MetricsRegistry metricsRegistry, AreciboProfile profile) {
        this(metricsRegistry, profile, null);
    }

    /**
     * Creates a new {@link AreciboMetricsReporter}.
     * 
     * @param metricsRegistry the metrics registry
     * @param profile the arecibo profile to keep up to date
     * @param mbeanExporter the mbean exporter to use for not-recognized metrics
     */
    public AreciboMetricsReporter(MetricsRegistry metricsRegistry, AreciboProfile profile, AreciboMBeanExporter mbeanExporter) {
        this.metricsRegistry = metricsRegistry;
        this.profile = profile;
        this.mbeanExporter = mbeanExporter;
        this.tickThread = metricsRegistry.newScheduledThreadPool(1, "arecibo-reporter");
    }

    /**
     * Starts this reporter.
     *
     * @param period the period between successive checks
     * @param unit   the time unit of {@code period}
     */
    public void start(long period, TimeUnit unit) {
        tickThread.scheduleAtFixedRate(this, 0, period, unit);
    }

    @Override
    public void run() {
        for (Map.Entry<MetricName, Metric> entry : metricsRegistry.allMetrics().entrySet()) {
            final MetricName name = entry.getKey();
            final Metric metric = entry.getValue();
    
            if (metric != null) {
                try {
                    if (metric instanceof Gauge<?>) {
                        registerGauge((Gauge<?>)metric, name);
                    }
                    else if (metric instanceof Counter) {
                        registerCounter((Counter)metric, name);
                    }
                    else if (metric instanceof Histogram) {
                        registerHistogram((Histogram)metric, name);
                    }
                    else if (metric instanceof Meter) {
                        registerMetered((Meter)metric, name);
                    }
                    else if (metric instanceof Timer) {
                        registerTimer((Timer)metric, name);
                    }
                    else if (mbeanExporter != null) {
                        // maybe it is annotated
                        mbeanExporter.export(name.getMBeanName(), metric);
                    }
                }
                catch (Exception ignored) {
                }
            }
        }
    }

    private String getEventName(MetricName name) {
        StringBuilder builder = new StringBuilder();

        builder.append(name.getGroup());
        builder.append(".");
        builder.append(name.getType());
        if (name.getScope() != null) {
            builder.append("-");
            builder.append(name.getScope());
        }
        if (name.getName().length() > 0) {
            builder.append("-");
            builder.append(name.getName());
        }
        return builder.toString();
    }

    private void registerValue(MetricName name, String attributeName, Class<?> type) {
        profile.add(name.getMBeanName(),
                    attributeName,
                    attributeName,
                    Monitored.DEFAULT_EVENT_NAME_PATTERN,
                    getEventName(name),
                    new MonitoringType[] { MonitoringType.VALUE },
                    type);
    }

    private void registerCounter(MetricName name, String attributeName, Class<?> type) {
        profile.add(name.getMBeanName(),
                    attributeName,
                    attributeName,
                    Monitored.DEFAULT_EVENT_NAME_PATTERN,
                    getEventName(name),
                    new MonitoringType[] { MonitoringType.COUNTER, MonitoringType.RATE },
                    type);
    }
    
    private void registerGauge(Gauge<?> metric, MetricName name) {
        profile.register(name.getMBeanName(), metric);
        registerValue(name, "Value", GaugeMBean.class);
    }

    private void registerCounter(Counter metric, MetricName name) {
        profile.register(name.getMBeanName(), metric);
        registerCounter(name, "Count", CounterMBean.class);
    }

    private void registerHistogram(Histogram metric, MetricName name) {
        profile.register(name.getMBeanName(), metric);
        registerCounter(name, "Count", HistogramMBean.class);
        registerValue(name, "Min", HistogramMBean.class);
        registerValue(name, "Max", HistogramMBean.class);
        registerValue(name, "Mean", HistogramMBean.class);
        registerValue(name, "StdDev", HistogramMBean.class);
        registerValue(name, "50thPercentile", HistogramMBean.class);
        registerValue(name, "75thPercentile", HistogramMBean.class);
        registerValue(name, "95thPercentile", HistogramMBean.class);
        registerValue(name, "98thPercentile", HistogramMBean.class);
        registerValue(name, "99thPercentile", HistogramMBean.class);
        registerValue(name, "999thPercentile", HistogramMBean.class);
    }

    private void registerMetered(Meter metric, MetricName name) {
        profile.register(name.getMBeanName(), metric);
        registerCounter(name, "Count", MeterMBean.class);
        registerValue(name, "MeanRate", MeterMBean.class);
        registerValue(name, "OneMinuteRate", MeterMBean.class);
        registerValue(name, "FiveMinuteRate", MeterMBean.class);
        registerValue(name, "FifteenMinuteRate", MeterMBean.class);
    }

    private void registerTimer(Timer metric, MetricName name) {
        profile.register(name.getMBeanName(), metric);
        registerCounter(name, "Count", TimerMBean.class);
        registerValue(name, "Min", TimerMBean.class);
        registerValue(name, "Max", TimerMBean.class);
        registerValue(name, "Mean", TimerMBean.class);
        registerValue(name, "StdDev", TimerMBean.class);
        registerValue(name, "50thPercentile", TimerMBean.class);
        registerValue(name, "75thPercentile", TimerMBean.class);
        registerValue(name, "95thPercentile", TimerMBean.class);
        registerValue(name, "98thPercentile", TimerMBean.class);
        registerValue(name, "99thPercentile", TimerMBean.class);
        registerValue(name, "999thPercentile", TimerMBean.class);
        registerValue(name, "MeanRate", TimerMBean.class);
        registerValue(name, "OneMinuteRate", TimerMBean.class);
        registerValue(name, "FiveMinuteRate", TimerMBean.class);
        registerValue(name, "FifteenMinuteRate", TimerMBean.class);
    }
}
