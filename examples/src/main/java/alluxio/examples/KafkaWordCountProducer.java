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

package alluxio.examples;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Generate unique words and publish them to a kafka topic.
 */
public class KafkaWordCountProducer {
  private static List<String> mDictionary = new ArrayList<>();
  private static Random mRandom = new Random();

  public static void main(String[] args) throws Exception {
    if (args.length < 5) {
      System.err.println(
          "Usage: JavaKafkaWordCount <brokerList> <topic> <messagesPerSec> <wordsPerMessage> "
              + "<maxUniqueMessageCount>");
      System.exit(1);
    }

    Logger.getRootLogger().setLevel(Level.ERROR);

    final String brokers = args[0];
    final String topic = args[1];
    final int messagesPerSeccond = Integer.parseInt(args[2]);
    final int wordsPerMessage = Integer.parseInt(args[3]);
    final long maxUniqueMessageCount = Long.parseLong(args[4]);

    Map<String, Object> configs = new HashMap<>();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

    // Build dictionary.
    for (int i = 0; i < maxUniqueMessageCount; i++) {
      StringBuilder sb = new StringBuilder();
      sb.append(i);
      sb.append("-");
      sb.append(mRandom.nextLong());
      mDictionary.add(sb.toString());
    }

    RateLimiter rateLimiter = RateLimiter.create(messagesPerSeccond);
    long counter = 0;
    while (true) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < wordsPerMessage; i++) {
        sb.append(mDictionary.get(i) + " ");
        counter++;
        counter %= maxUniqueMessageCount;
      }
      ProducerRecord<String, String> message = new ProducerRecord<>(topic, null, sb.toString());
      producer.send(message);
      rateLimiter.acquire();
    }
  }
}
