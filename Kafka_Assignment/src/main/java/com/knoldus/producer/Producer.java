package com.knoldus.producer;

import com.knoldus.model.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

            /*A program to produced the data */

public class Producer {
    public static void main(String[] args){
        
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.knoldus.serializer.UserSerializer");
        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);
        try {
            Random rand = new Random();

            /*Method to build the Data*/
            for (int i = 1; i <= 100; i++) {

                User user = new User(i, "Vikas", rand.nextInt(10)+20, "MCA");
                producer.send(new ProducerRecord<String, User>("user-add", String.valueOf(i), user));
                System.out.println("Message " + user.toString() + " sent !!");
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
