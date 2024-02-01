package com.example.rabbitmqsample.service;

import com.example.rabbitmqsample.dto.MessageDto;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class MessageService {
  /**
   * Queue 로 메세지를 발행할 때에는 RabbitTemplate 의 ConvertAndSend 메소드를 사용하고
   * Queue 에서 메세지를 구독할때는 @RabbitListener 을 사용
   *
   **/

  @Value("${rabbitmq.exchange.name}")
  private String exchangeName;

  @Value("${rabbitmq.routing.key}")
  private String routingKey;

  private final RabbitTemplate rabbitTemplate;

  /**
   * Queue로 메시지를 발행
   *
   * 이렇게 메세지를 발행하면 Direct Exchange 전략에 따라 주어진 Routing Key 로 바인딩된 Queue 로 메세지가 들어가게 되고,
   * 이는 해당 Queue 을 구독하는 Consumer(어플리케이션) 으로 들어가게 된다.
   *
   * @param messageDto 발행할 메시지의 DTO 객체
   */
  public void sendMessage(MessageDto messageDto) {
    log.info("message sent: {}", messageDto.toString());
    rabbitTemplate.convertAndSend(exchangeName, routingKey, messageDto);
  }

  /**
   * Queue에서 메시지를 구독
   *
   * @param messageDto 구독한 메시지를 담고 있는 MessageDto 객체
   */
  @RabbitListener(queues = "${rabbitmq.queue.name}")
  public void reciveMessage(MessageDto messageDto) {
    log.info("Received message: {}", messageDto.toString());
  }
}
