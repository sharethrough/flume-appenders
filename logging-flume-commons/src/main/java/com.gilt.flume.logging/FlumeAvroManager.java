package com.gilt.flume.logging;

import org.apache.flume.Event;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FlumeAvroManager {

  private final LoggingAdapter logger;

  private final LoggingAdapterFactory loggerFactory;

  private static final AtomicLong threadSequence = new AtomicLong(1);

  private static final int MAX_RECONNECTS = 3;
  private static final int MINIMUM_TIMEOUT = 1000;

  private final static long MAXIMUM_REPORTING_MILLIS = 10 * 1000;
  private final static long MINIMUM_REPORTING_MILLIS = 100;
  private final static long DEFAULT_REPORTER_CONNECTION_RESET_INTERVAL_MILLIS = 60 * 1000;
  private final static int DEFAULT_BATCH_SIZE = 50;
  private final static int DEFAULT_REPORTER_MAX_THREADPOOL_SIZE = 2;
  private final static int DEFAULT_REPORTER_MAX_QUEUE_SIZE = 50;

  private final BlockingQueue<Event> evQueue;

  private final AsyncThread asyncThread;

  private final EventReporter reporter;

  public static FlumeAvroManager create(
      final List<RemoteFlumeAgent> agents,
      final Properties overrides,
      final Integer batchSize,
      final Long reportingWindow,
      final Long connectionResetInterval,
      final Integer reporterMaxThreadPoolSize,
      final Integer reporterMaxQueueSize,
      final LoggingAdapterFactory loggerFactory) {

      if (agents != null && agents.size() > 0) {
        Properties props = buildFlumeProperties(agents);
        props.putAll(overrides);
        return new FlumeAvroManager(props, reportingWindow, connectionResetInterval, batchSize, reporterMaxThreadPoolSize,
              reporterMaxQueueSize, loggerFactory);
      } else {
        loggerFactory.create(FlumeAvroManager.class).error("No valid agents configured");
      }

    return null;
  }

  private FlumeAvroManager(final Properties props,
                           final Long reportingWindowReq,
                           final Long connectionResetInterval,
                           final Integer batchSizeReq,
                           final Integer reporterMaxThreadPoolSizeReq,
                           final Integer reporterMaxQueueSizeReq,
                           final LoggingAdapterFactory loggerFactory) {

    this.logger = loggerFactory.create(FlumeAvroManager.class);
    this.loggerFactory = loggerFactory;
    final int reporterMaxThreadPoolSize = reporterMaxThreadPoolSizeReq == null ?
            DEFAULT_REPORTER_MAX_THREADPOOL_SIZE : reporterMaxThreadPoolSizeReq;
    final int reporterMaxQueueSize = reporterMaxQueueSizeReq == null ?
            DEFAULT_REPORTER_MAX_QUEUE_SIZE : reporterMaxQueueSizeReq;
    final long reporterConnectionResetInterval = connectionResetInterval == null ?
            DEFAULT_REPORTER_CONNECTION_RESET_INTERVAL_MILLIS : connectionResetInterval;

    this.reporter = new EventReporter(props, reporterMaxThreadPoolSize, reporterMaxQueueSize, loggerFactory, reporterConnectionResetInterval);
    this.evQueue = new ArrayBlockingQueue<Event>(1000);
    final long reportingWindow = hamonizeReportingWindow(reportingWindowReq);
    final int batchSize = batchSizeReq == null ? DEFAULT_BATCH_SIZE : batchSizeReq;
    this.asyncThread = new AsyncThread(evQueue, batchSize, reportingWindow);
    logger.info("Created a new flume agent with properties: " + props.toString());
    asyncThread.start();
  }

  private long hamonizeReportingWindow(Long reportingWindowReq) {
    if(reportingWindowReq == null)
      return MAXIMUM_REPORTING_MILLIS;

    if(reportingWindowReq > MAXIMUM_REPORTING_MILLIS)
      return MAXIMUM_REPORTING_MILLIS;

    if( reportingWindowReq < MINIMUM_REPORTING_MILLIS)
      return MINIMUM_REPORTING_MILLIS;

    return reportingWindowReq;
  }

  public void stop() {
    asyncThread.shutdown();
  }

  public void send(Event event) {
    if (event != null) {
      evQueue.add(event);
    } else {
      logger.warn("Trying to send a NULL event");
    }
  }

  private static Properties buildFlumeProperties(List<RemoteFlumeAgent> agents) {
    Properties props = new Properties();

    int i = 0;
    for (RemoteFlumeAgent agent : agents) {
      props.put("hosts.h" + (i++), agent.getHostname() + ':' + agent.getPort());
    }
    StringBuffer buffer = new StringBuffer(i * 4);
    for (int j = 0; j < i; j++) {
      buffer.append("h").append(j).append(" ");
    }
    props.put("hosts", buffer.toString());
    props.put("max-attempts", Integer.toString(MAX_RECONNECTS * agents.size()));

    props.put("request-timeout", Integer.toString(MINIMUM_TIMEOUT));
    props.put("connect-timeout", Integer.toString(MINIMUM_TIMEOUT));

    if(i > 1) {
      props.put("client.type", "default_loadbalance");
      props.put("host-selector", "round_robin");
    }

    props.put("backoff", "true");
    props.put("maxBackoff", "10000");

    return props;
  }

  /**
   * Pulls events from the queue and offloads the resulting array of events
   * to a thread pool(?) that sends that batch to a flume agent
   */
  private class AsyncThread extends Thread {

    private final BlockingQueue<Event> queue;
    private final long reportingWindow;
    private final int  batchSize;
    private volatile boolean shutdown = false;

    private AsyncThread(final BlockingQueue<Event> queue, final int batchSize, final long reportingWindow) {
      this.queue = queue;
      this.batchSize = batchSize;
      this.reportingWindow = reportingWindow;
      setDaemon(true);
      setName("FlumeAvroManager-" + threadSequence.getAndIncrement());
      logger.info("Started a new " + AsyncThread.class.getSimpleName() + " thread");
    }

    @Override
    public void run() {
      while (!shutdown) {
        long lastPoll = System.currentTimeMillis();
        long maxTime = lastPoll + reportingWindow;
        final Event[] events = new Event[batchSize];
        int count = 0;
        try {
          while (count < batchSize && System.currentTimeMillis() < maxTime) {
            lastPoll = Math.max(System.currentTimeMillis(), lastPoll); // Corrects to last seen time if clock
                                                                       // moves backwards
            Event ev = queue.poll(maxTime - lastPoll, TimeUnit.MILLISECONDS);
            if (ev != null) {
              events[count++] = ev;
            }
          }
        } catch (InterruptedException ie) {
          logger.warn(ie.getLocalizedMessage(), ie);
        }
        if(count > 0) {
          Event[] batch;
          if (count == events.length) {
            batch = events;
          } else {
            batch = new Event[count];
            System.arraycopy(events, 0, batch, 0, count);
          }
          try{
            reporter.report(batch);
          } catch (RejectedExecutionException ex) {
            logger.error("Logging events batch rejected by EventReporter. Check reporter connectivity or " +
                    "consider increasing reporterMaxThreadPoolSize or reporterMaxQueueSize", ex);
          }
        }
      }

      reporter.shutdown();
    }

    public void shutdown() {
      logger.error("Shutting down command received");
      shutdown = true;
    }
  }
}
