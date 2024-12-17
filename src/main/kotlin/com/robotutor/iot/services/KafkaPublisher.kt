package com.robotutor.iot.services

import com.robotutor.iot.models.KafkaTopicName
import com.robotutor.iot.models.Message
import com.robotutor.loggingstarter.LogDetails
import com.robotutor.loggingstarter.Logger
import com.robotutor.loggingstarter.serializer.DefaultSerializer
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class KafkaPublisher(
    private val kafkaTemplate: KafkaTemplate<String, String>,
) {
    val logger = Logger(this::class.java)
    fun publish(topicName: KafkaTopicName, key: String? = null, message: Message) {
        val messageAsString = DefaultSerializer.serialize(message)
        if (key != null) {
            kafkaTemplate.send(topicName.toString(), key, messageAsString)
        } else {
            kafkaTemplate.send(topicName.toString(), messageAsString)
        }
        logger.info(LogDetails(message = "Successfully published topic $topicName"))
    }
}
