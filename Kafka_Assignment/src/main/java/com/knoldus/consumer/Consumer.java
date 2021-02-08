package com.knoldus.consumer;

import com.knoldus.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.FileWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

        /*A program to consume the produced data and storing in a file "store-data.txt"*/

public class Consumer {
    public static void main(String[] args) {
        ConsumerListener c = new ConsumerListener();
        Thread thread = new Thread(c);
        thread.start();
    }
    public static void consumer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "com.knoldus.deserializer.UserDeserializer");
        properties.put("group.id", "test-group");

        KafkaConsumer<String, User> kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("user-add");
        kafkaConsumer.subscribe(topics);
        try{
            // Message 1
            FileWriter file = new FileWriter("store-data.txt");
            while (true){
                FileWriter fileWriter = new FileWriter("store-data.txt",true);
                ConsumerRecords<String, User> records = kafkaConsumer.poll(Duration.ofSeconds(1000));
                ObjectMapper mapper = new ObjectMapper();

                /* Method that would be writing this value into a file. */

                for (ConsumerRecord<String, User> record: records){
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value().toString()));
                    System.out.println(mapper.writeValueAsString(record.value()));
                    fileWriter.append(mapper.writeValueAsString(record.value())+"\n");
                 }
                fileWriter.close();
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {

            kafkaConsumer.close();
        }
    }
}

class ConsumerListener implements Runnable {


    @Override
    public void run() {
        Consumer.consumer();
    }
}
