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
import com.codahale.metrics.MetricRegistry;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Worker source collects a worker's internal state.
 */
@ThreadSafe
public final class ClientSource implements Source {
  private static final String CLIENT_SOURCE_NAME = "client";

  public static final String SEEKS_LOCAL = "SeeksLocal";
  public static final String SEEKS_REMOTE = "SeeksRemote";

  public static final MetricRegistry mMetricRegistry = new MetricRegistry();
  public final Counter mSeeksLocal = mMetricRegistry.counter(MetricRegistry.name(SEEKS_LOCAL));
  public final Counter mSeeksRemote = mMetricRegistry.counter(MetricRegistry.name(SEEKS_REMOTE));

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
    return mMetricRegistry;
  }
}
