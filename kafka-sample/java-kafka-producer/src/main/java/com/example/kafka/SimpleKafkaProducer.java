package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

/**
 * Kafka Producer.
 * keyboard input을 통해 메시지를 전송한다.
 *
 */
public class SimpleKafkaProducer {

    private final static String TOPIC_NAME = "bum-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String FIN_MESSAGE = "exit";


    public static void main(String[] args) {
        // Kafka Consumer 초기화에 필요한 설정 세팅. 관련된 설정 key값은 ProducerConfig 클래스에서 제공
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); //Kafka 클러스터 IP 목록을 입력해 준다.
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //레코드의 메시지 키를 직렬화하는 클래스를 지정한다.
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //레코드의 메시지 값를 직렬화하는 클래스를 지정한다.
        // 카프카의 모든 메세지는 직렬화된 상태로 전송되어야함. StringSerializer 이외에도 JSON, Apache Abro 등 사용할 수도 있다.

        // Properties 클래스를 사용해서 옵션값을 설정하여 Kafka Producer 인스턴스를 초기화
        Producer<String, String> producer = new KafkaProducer<>(properties);

        while (true) {
            Scanner sc = new Scanner(System.in);
            System.out.print("> ");
            String message = sc.nextLine();

            //입력받은 메시지를 Kafka 토픽에 전송하기 위한 객체(전송할 메시지와 목적지 토픽 설정)
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, message);
            try {
                // 메시지를 Kafka 토픽으로 전송(메시지 전송 후에는 메시지가 브로커에게 실제로 전송되지 않고, 내부적으로 배치되어 나중에 한번에 브로커로 전송)
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });
            } finally {
                //현재까지 누적된 모든 메시지를 브로커로 전송 (배치로 전송)
                //따로 호출하지 않아도 일정한 시간 간격이나 일정한 메시지 크기에 도달하면 내부적으로 자동으로 배치가 전송되기도 함.
                producer.flush();
            }

            if(StringUtils.equals(message, FIN_MESSAGE)) {
                producer.close();
                break;
            }
        }
    }
}
