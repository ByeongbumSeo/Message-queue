package com.example.activemq.config;

import com.example.activemq.dto.MessageDto;
import jakarta.jms.Queue;
import java.util.HashMap;
import java.util.Map;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;

@Configuration
public class ActiveMQConfig {

  @Value("${spring.activemq.broker-url}")
  private String activemqBrokerUrl;

  @Value("${spring.activemq.user}")
  private String activemqUsername;

  @Value("${spring.activemq.password}")
  private String activemqPassword;

  @Value("${activemq.queue.name}")
  private String queueName;

  /**
   * 지정된 큐 이름으로 Queue 빈을 생성
   *
   * @return Queue 빈 객체
   */
  @Bean
  public Queue queue() {
    return new ActiveMQQueue(queueName);
  }

  /**
   * ActiveMQ 연결을 위한 ActiveMQConnectionFactory 빈을 생성하여 반환
   *
   * @return ActiveMQConnectionFactory 객체
   */
  @Bean
  public ActiveMQConnectionFactory activeMQConnectionFactory() {
    ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
    activeMQConnectionFactory.setBrokerURL(activemqBrokerUrl);
    activeMQConnectionFactory.setUserName(activemqUsername);
    activeMQConnectionFactory.setPassword(activemqPassword);
    return activeMQConnectionFactory;
  }

  /**
   * JmsTemplate을 생성하여 반환
   *
   * @return JmsTemplate 객체
   */
  @Bean
  public JmsTemplate jmsTemplate() {
    JmsTemplate jmsTemplate = new JmsTemplate(activeMQConnectionFactory());
    jmsTemplate.setMessageConverter(jacksonJmsMessageConverter());
    jmsTemplate.setExplicitQosEnabled(true);    // 메시지 전송 시 QOS을 설정
    jmsTemplate.setDeliveryPersistent(false);   // 메시지의 영속성을 설정
    jmsTemplate.setReceiveTimeout(1000 * 3);    // 메시지를 수신하는 동안의 대기 시간을 설정(3초)
    jmsTemplate.setTimeToLive(1000 * 60 * 30);  // 메시지의 유효 기간을 설정(30분)
    return jmsTemplate;
  }

  /**
   * JmsListenerContainerFactory을 위한 빈을 생성
   *
   * @return JmsTemplate
   */
  @Bean
  public JmsListenerContainerFactory<?> jmsListenerContainerFactory() {
    DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
    factory.setConnectionFactory(activeMQConnectionFactory());
    factory.setMessageConverter(jacksonJmsMessageConverter());
    return factory;
  }

  /**
   * MessageConverter을 위한 빈을 생성
   *
   * @return MessageConverter
   */
  @Bean
  public MessageConverter jacksonJmsMessageConverter() {
    MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
    converter.setTargetType(MessageType.TEXT);
    converter.setTypeIdPropertyName("_typeId");
    Map<String, Class<?>> typeIdMappings = new HashMap<>();
    typeIdMappings.put("message", MessageDto.class);
    converter.setTypeIdMappings(typeIdMappings);
    return converter;
  }
}
