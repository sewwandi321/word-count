package org.example.ns;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;


public class WordCountApp {

    //private static final Logger logger = (Logger) LoggerFactory.getLogger(WordCountApp.class);
    private  static final Logger logger = LoggerFactory.getLogger(WordCountApp.class);
    public static void main(String[] args) {
        Properties props = new Properties();
        //we can have many consumers belongs to same consumer group
        //so when new msg  write to a topic every consumer will be defined to different partition
        //and if they are in the same consumer group they will consume from different partitions
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"word-count-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");



        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // Build the Topology
//        streamsBuilder.<String, String>stream("sentences")
//                .flatMapValues((key, value) ->
//                        Arrays.asList(value.toLowerCase()
//                                .split(" ")))
//                .groupBy((key, value) -> value)
//                .count(Materialized.with(Serdes.String(), Serdes.Long()))
//                .toStream()
//                .to("word-count", Produced.with(Serdes.String(), Serdes.Long()));
//
//
        KStream<String, Long> wordCounts = streamsBuilder.stream("sentences",Consumed.with(Serdes.String(),Serdes.String()))
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count()
                .toStream()
                .peek((k, v) -> logger.info("Ingested2: key => {}, val => {}", k, v));

        wordCounts.to("word-count", Produced.with(Serdes.String(), Serdes.Long()));

        // Create the Kafka Streams Application
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        // Start the application
        kafkaStreams.start();

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));


    }
}
