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

import com.google.common.io.Files;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InternalMapWithStateDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Counts words cumulatively in UTF8 encoded, '\n' delimited text received from the network every
 * second starting with initial value of word count.
 * Usage: JavaStatefulNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
 * data.
 * <p>
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example
 * org.apache.spark.examples.streaming.JavaStatefulNetworkWordCount localhost 9999`
 */
public class StatefulKafkaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
    if (args.length < 7) {
      System.err.println(
          "Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads> <batchSize> "
              + "<outputPath> <checkpoint>");
      System.exit(1);
    }

    Logger.getRootLogger().setLevel(Level.ERROR);

    final String zkQuorum = args[0];
    final String group = args[1];
    final String topics = args[2];
    final int numThreads = Integer.parseInt(args[3]);
    final int batchSize = Integer.parseInt(args[4]);
    final String outputPath = args[5];
    final String checkpointDirectory = args[6];

    final File outputFile = new File(outputPath);
    if (outputFile.exists()) {
      outputFile.delete();
    }

    SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchSize));
    ssc.checkpoint(checkpointDirectory);

    Map<String, Integer> topicMap = new HashMap<>();
    for (String topic : topics.split(",")) {
      topicMap.put(topic, numThreads);
    }

    JavaPairReceiverInputDStream<String, String> messages =
        KafkaUtils.createStream(ssc, zkQuorum, group, topicMap);

    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
      @Override
      public String call(Tuple2<String, String> tuple2) {
        return tuple2._2();
      }
    });

    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String x) {
        return Arrays.asList(SPACE.split(x)).iterator();
      }
    });

    // Initial state RDD input to mapWithState
    @SuppressWarnings("unchecked")
    List<Tuple2<String, Integer>> tuples =
        Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));
    JavaPairRDD<String, Integer> initialRDD = ssc.sparkContext().parallelizePairs(tuples);

    JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) {
            return new Tuple2<>(s, 1);
          }
        });

    // Update the cumulative count function
    Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
        new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
          @Override
          public Tuple2<String, Integer> call(String word, Optional<Integer> one,
              State<Integer> state) {
            int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
            Tuple2<String, Integer> output = new Tuple2<>(word, sum);
            state.update(sum);
            return output;
          }
        };

    // DStream made of get cumulative counts that get updated in every batch
    JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
        wordsDstream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));

    stateDstream.print();

    wordsDstream.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
      @Override
      public void call(JavaPairRDD<String, Integer> rdd, Time time) throws IOException {
        // Use blacklist to drop words and use droppedWordsCounter to count them
        String counts = rdd.filter(new Function<Tuple2<String, Integer>, Boolean>() {
              @Override
              public Boolean call(Tuple2<String, Integer> wordCount) {
                return true;
              }
        }).collect().toString();
        String output = "Counts at time " + time + " " + counts;
        System.out.println(output);
        System.out.println("Appending to " + outputFile.getAbsolutePath());
        Files.append(output + "\n", outputFile, Charset.defaultCharset());
      }
    });
    ssc.start();
    ssc.awaitTermination();
  }
}
