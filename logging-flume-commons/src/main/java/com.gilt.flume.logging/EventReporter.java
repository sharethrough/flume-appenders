package com.gilt.flume.logging;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.*;

public class EventReporter {

  private RpcClient client;

  private final LoggingAdapter logger;

  private final ExecutorService es;

  private final Properties connectionProps;

  private final long connectionResetInterval;

  private long connectionTimestamp;

  public EventReporter(final Properties properties, final int maximumThreadPoolSize, final int maxQueueSize,
                       final LoggingAdapterFactory loggingFactory, final long connectionResetInterval) {
    this.logger = loggingFactory.create(EventReporter.class);
    BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<Runnable>(maxQueueSize);
    this.connectionProps = properties;
    this.connectionResetInterval = connectionResetInterval;

    int corePoolSize = 1;
    TimeUnit threadKeepAliveUnits = TimeUnit.SECONDS;
    int threadKeepAliveTime = 30;
    RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();

    this.es = new ThreadPoolExecutor(corePoolSize, maximumThreadPoolSize, threadKeepAliveTime,
            threadKeepAliveUnits, blockingQueue, handler);
  }

  public void report(final Event[] events) {
    es.submit(new ReportingJob(events));
  }

  private synchronized RpcClient createClient() {
    long now = System.currentTimeMillis();

    // Force reset connection
    if (connectionResetInterval > 0 &&
        now - connectionTimestamp > connectionResetInterval) {
      logger.info(String.format("Resetting connection since %d - %d > %d", now, connectionTimestamp, connectionResetInterval));
      close();
    }

    if (client == null) {
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

  private class ReportingJob implements Runnable {

    private static final int retries = 3;

    private final Event[] events;

    public ReportingJob(final Event[] events) {
      this.events = events;
      logger.debug("Created a job containing {} events", events.length);
    }


    @Override
    public void run() {
      boolean success = false;
      int count = 0;
      try {
        while (!success && count < retries) {
          count++;
          try {
            logger.debug("Reporting a batch of {} events, try {}", events.length, count);
            createClient().appendBatch(Arrays.asList(events));
            success = true;
            logger.debug("Successfully reported a batch of {} events", events.length);
          } catch (EventDeliveryException e) {
            logger.warn(e.getLocalizedMessage(), e);
            logger.warn("Will retry " + (retries - count) + " times");
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
