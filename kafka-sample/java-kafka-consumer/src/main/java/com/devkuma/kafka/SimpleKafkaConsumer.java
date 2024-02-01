package com.devkuma.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


/**
 * Kafka Consumer.
 * Producer가 전송한 메시지를 받는다.
 *
 */
public class SimpleKafkaConsumer {

    //소비할 토픽의 이름
    private final static String TOPIC_NAME = "bum-topic";
    //브로커 주소
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {

        //Kafka Consumer 초기화에 필요한 설정 세팅
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); //Kafka 클러스터 IP 목록을 입력해 준다.
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //레코드의 메시지 키를 역직렬화하는 클래스를 지정한다.
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //레코드의 메시지 값를 역직렬화하는 클래스를 지정한다.
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, TOPIC_NAME); //Consumer Group의 ID

        // Properties 클래스를 사용해서 옵션값을 설정하여 Kafka Consumer 인스턴스를 초기화
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        // subscribe 메서드로 소비할 토픽 구독 시작
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        String message = null;
        try {
            do {
                //poll 메서드를 사용하여 지정된 시간동안 대기하면서 메시지 소비
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100000)); //레코드를 기다릴 최대 시간

                for (ConsumerRecord<String, String> record : records) {
                    message = record.value();
                    System.out.println(message);
                }
            } while (!message.equals("/quit"));
        } finally {
            consumer.close();
        }
    }
}
