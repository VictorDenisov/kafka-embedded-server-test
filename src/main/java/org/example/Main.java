package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import java.time.Duration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        String testTopic = "test-topic";
        EmbeddedKafkaCluster embeddedKafkaCluster = new EmbeddedKafkaCluster(1);
        System.out.println("Before server start");
        embeddedKafkaCluster.start();
        System.out.println("After server start");
        embeddedKafkaCluster.createTopic(testTopic);
        System.out.println("After topic create");

        String bootstrapServers = embeddedKafkaCluster.bootstrapServers();
        System.out.println("Retrieved bootstrap server: " + bootstrapServers);

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        System.out.println("Constructing producer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig);
        System.out.println("Sending test key value");
        Future<RecordMetadata> f = producer.send(new ProducerRecord<>(testTopic, "key", "value"));

        RecordMetadata metadata = f.get();

        System.out.println("Sent test key value" + metadata);

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-tutorial");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Arrays.asList(testTopic));
        ConsumerRecords<String, String> records;
        for (records = consumer.poll(Duration.ofSeconds(10)); ;records = consumer.poll(Duration.ofSeconds(10))) {
            for (ConsumerRecord<String, String> record: records) {
                System.out.println(record.value());
            }
        }
    }
}
