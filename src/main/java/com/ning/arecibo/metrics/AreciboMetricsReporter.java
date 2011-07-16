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
                    if (metric instanceof GaugeMetric<?>) {
                        registerGauge((GaugeMetric<?>)metric, name);
                    } else if (metric instanceof CounterMetric) {
                        registerCounter((CounterMetric)metric, name);
                    } else if (metric instanceof HistogramMetric) {
                        registerHistogram((HistogramMetric)metric, name);
                    } else if (metric instanceof MeterMetric) {
                        registerMetered((MeterMetric)metric, name);
                    } else if (metric instanceof TimerMetric) {
                        registerTimer((TimerMetric)metric, name);
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
    
    private void registerGauge(GaugeMetric<?> metric, MetricName name) {
        profile.register(name.getMBeanName(), metric);
        registerValue(name, "Value", GaugeMBean.class);
    }

    private void registerCounter(CounterMetric metric, MetricName name) {
        profile.register(name.getMBeanName(), metric);
        registerCounter(name, "Count", CounterMBean.class);
    }

    private void registerHistogram(HistogramMetric metric, MetricName name) {
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

    private void registerMetered(MeterMetric metric, MetricName name) {
        profile.register(name.getMBeanName(), metric);
        registerCounter(name, "Count", MeterMBean.class);
        registerValue(name, "MeanRate", MeterMBean.class);
        registerValue(name, "OneMinuteRate", MeterMBean.class);
        registerValue(name, "FiveMinuteRate", MeterMBean.class);
        registerValue(name, "FifteenMinuteRate", MeterMBean.class);
    }

    private void registerTimer(TimerMetric metric, MetricName name) {
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
