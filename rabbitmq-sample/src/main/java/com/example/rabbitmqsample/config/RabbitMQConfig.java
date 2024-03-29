package com.example.rabbitmqsample.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

  @Value("${spring.rabbitmq.host}")
  private String rabbitmqHost;

  @Value("${spring.rabbitmq.port}")
  private int rabbitmqPort;

  @Value("${spring.rabbitmq.username}")
  private String rabbitmqUsername;

  @Value("${spring.rabbitmq.password}")
  private String rabbitmqPassword;

  @Value("${rabbitmq.queue.name}")
  private String queueName;

  @Value("${rabbitmq.exchange.name}")
  private String exchangeName;

  @Value("${rabbitmq.routing.key}")
  private String routingKey;

  /**
   * 지정된 큐 이름으로 Queue 빈을 생성
   *
   * @return Queue 빈 객체
   */
  @Bean
  public Queue queue() {
    return new Queue(queueName);
  } // org.springframework.amqp.core.Queue

  /**
   * 지정된 Exchange 이름으로 DirectExchange 빈을 생성
   *
   * @return TopicExchange 빈 객체
   */
  @Bean
  public DirectExchange exchange() {
    return new DirectExchange(exchangeName);
  }

  /**
   * 주어진 Queue 와 Exchange 을 Binding 하고 Routing Key 을 이용하여 Binding Bean 생성
   * Exchange 에 Queue 을 등록한다고 이해
   *
   * @param queue    바인딩할 Queue
   * @param exchange 바인딩할 TopicExchange
   * @return Binding 빈 객체
   */
  @Bean
  public Binding binding(Queue queue, DirectExchange exchange) {
    return BindingBuilder.bind(queue).to(exchange).with(routingKey);
  }

  /**
   * RabbitMQ 연결을 위한 ConnectionFactory 빈을 생성하여 반환
   *
   * @return ConnectionFactory 객체
   */
  @Bean
  public ConnectionFactory connectionFactory() {
    CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
    connectionFactory.setHost(rabbitmqHost);
    connectionFactory.setPort(rabbitmqPort);
    connectionFactory.setUsername(rabbitmqUsername);
    connectionFactory.setPassword(rabbitmqPassword);
    return connectionFactory;
  }

  /**
   * RabbitTemplate을 생성하여 반환
   *
   * ConnectionFactory 로 연결 후 실제 작업을 위한 Template
   *
   * @param connectionFactory RabbitMQ와의 연결을 위한 ConnectionFactory 객체
   * @return RabbitTemplate 객체
   */
  @Bean
  public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
    RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
    // JSON 형식의 메시지를 직렬화하고 역직렬할 수 있도록 설정
    rabbitTemplate.setMessageConverter(jackson2JsonMessageConverter());
    return rabbitTemplate;
  }

  /**
   * Jackson 라이브러리를 사용하여 메시지를 JSON 형식으로 변환하는 MessageConverter 빈을 생성
   *
   * @return MessageConverter 객체
   */
  @Bean
  public MessageConverter jackson2JsonMessageConverter() {
    return new Jackson2JsonMessageConverter();
  }
}
