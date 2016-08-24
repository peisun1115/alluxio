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

import alluxio.metrics.source.Source;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Worker source collects a worker's internal state.
 */
@ThreadSafe
public final class ClientSource implements Source {
  private static final String CLIENT_SOURCE_NAME = "client";

  public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
  public static final Counter SEEKS_LOCAL =
      METRIC_REGISTRY.counter(MetricRegistry.name("SeeksLocal"));
  public static final Counter SEEKS_REMOTE =
      METRIC_REGISTRY.counter(MetricRegistry.name("SeeksRemote"));

  public static final Histogram READ_SIZES =
      METRIC_REGISTRY.histogram(MetricRegistry.name("ReadSizes"));
  public static final Histogram SEEK_LATENCY =
      METRIC_REGISTRY.histogram(MetricRegistry.name("SeekLatency"));
  public static final Histogram READ_LATENCY =
      METRIC_REGISTRY.histogram(MetricRegistry.name("ReadLatency"));

  /**
   * Constructs a new {@link ClientSource}.
   */
  public ClientSource() {}

  @Override
  public String getName() {
    return CLIENT_SOURCE_NAME;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return METRIC_REGISTRY;
  }
}
