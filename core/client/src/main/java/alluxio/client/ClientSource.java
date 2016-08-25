/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client;

import alluxio.Constants;
import alluxio.metrics.source.Source;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Worker source collects a worker's internal state.
 */
@ThreadSafe
public final class ClientSource implements Source {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final String CLIENT_SOURCE_NAME = "client";

  private final MetricRegistry mMetricRegistry = new MetricRegistry();

  // TODO(peis): Move all the metrics in ClientMetrics here.
  private final Counter mBytesRead = mMetricRegistry.counter(MetricRegistry.name("BytesRead"));
  private final Timer mFileSeeks = mMetricRegistry.timer(MetricRegistry.name("FileSeeks"));

  /**
   * Constructs a new {@link ClientSource}.
   */
  public ClientSource() {
    LOG.info("Client source is initialized.");
  }

  @Override
  public String getName() {
    return CLIENT_SOURCE_NAME;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return mMetricRegistry;
  }

  public Counter getBytesRead() {
    return mBytesRead;
  }

  public Timer getFileSeeks() {
    return mFileSeeks;
  }
}
