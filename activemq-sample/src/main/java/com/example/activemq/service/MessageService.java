package com.example.activemq.service;


import com.example.activemq.dto.MessageDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class MessageService {

  @Value("${activemq.queue.name}")
  private String queueName;

  private final JmsTemplate jmsTemplate;

  /**
   * Queue로 메시지를 발행
   *
   * @param messageDto 발행할 메시지의 DTO 객체
   */
  public void sendMessage(MessageDto messageDto) {
    log.info("message sent: {}", messageDto.toString());
    jmsTemplate.convertAndSend(queueName, messageDto);
  }

  /**
   * Queue에서 메시지를 구독
   *
   * @param messageDto 구독한 메시지를 담고 있는 MessageDto 객체
   */
  @JmsListener(destination = "${activemq.queue.name}")
  public void reciveMessage(MessageDto messageDto) {
    log.info("Received message: {}", messageDto.toString());
  }
}
