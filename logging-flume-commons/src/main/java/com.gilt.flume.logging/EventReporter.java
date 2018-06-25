package com.gilt.flume.logging;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

public class EventReporter {

  private RpcClient client;
  private final LoggingAdapter logger;
  private final ExecutorService es;
  private final Properties connectionProps;
  private final long connectionResetInterval;
  private long connectionTimestamp;
  private final int retryCount;

  public EventReporter(final Properties properties, final int maximumThreadPoolSize, final int maxQueueSize,
                       final LoggingAdapterFactory loggingFactory, final long connectionResetInterval,
                       final int retryCount) {
    this.logger = loggingFactory.create(EventReporter.class);
    BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<Runnable>(maxQueueSize);
    this.connectionProps = properties;
    this.connectionResetInterval = connectionResetInterval;
    this.retryCount = retryCount;

    int corePoolSize = 1;
    TimeUnit threadKeepAliveUnits = TimeUnit.SECONDS;
    int threadKeepAliveTime = 30;
    RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();

    this.es = new ThreadPoolExecutor(corePoolSize, maximumThreadPoolSize, threadKeepAliveTime,
            threadKeepAliveUnits, blockingQueue, handler);
  }

  public void report(final List<Event> events) {
    es.submit(new ReportingJob(events, retryCount));
  }

  private synchronized RpcClient createClient() {
    long now = System.currentTimeMillis();

    // Force reset connection
    if (shouldResetConnection(now)) {
      logger.info(String.format("Resetting connection since %d - %d > %d", now, connectionTimestamp,
              connectionResetInterval));
      close();
    }

    if (!isClientUsable()) {
      close();
      logger.info("Creating a new Flume Client with properties: " + connectionProps);
      try {
        client = RpcClientFactory.getInstance(connectionProps);
        connectionTimestamp = System.currentTimeMillis();
      } catch ( Exception e ) {
        logger.error(e.getLocalizedMessage(), e);
      }
    }

    return client;
  }

  public synchronized void close() {
    logger.info("Shutting down Flume client");
    if (client != null) {
      client.close();
      client = null;
    }
  }

  public void shutdown() {
    close();
    es.shutdown();
  }

  private boolean shouldResetConnection(long now) {
    return client != null &&
            connectionResetInterval > 0 &&
            now - connectionTimestamp > connectionResetInterval;
  }

  private boolean isClientUsable() {
    return client != null && client.isActive();
  }


  private class ReportingJob implements Runnable {
    private final List<Event> events;
    private final int retries;

    public ReportingJob(final List<Event> events, int retries) {
      this.events = events;
      this.retries = retries;
      logger.debug("Created a job containing {} events", events.size());
    }

    private boolean shouldRun(boolean success, int attemptCount) {
      return !success && (retries < 0 || attemptCount < retries);
    }

    @Override
    public void run() {
      boolean success = false;
      try {
        for (int count = 0; shouldRun(success, count); count += 1) {
          count++;
          try {
            logger.debug("Reporting a batch of {} events, try {}", events.size(), count);
            createClient().appendBatch(events);
            success = true;
            logger.debug("Successfully reported a batch of {} events", events.size());
          } catch (EventDeliveryException e) {
            logger.warn(e.getLocalizedMessage(), e);
            if (retries < 0) {
              logger.warn("Will retry indefinitely.  Already tied " + count + " times");
            } else if (count < retries) {
              logger.warn("Will retry up to " + (retries - count) + " more times");
            }
          }
        }
      } finally {
        if (!success) {
          logger.error("Could not submit events to Flume");
          close();
        }
      }
    }
  }
}
