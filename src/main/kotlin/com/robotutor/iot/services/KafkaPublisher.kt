package com.robotutor.iot.services

import com.robotutor.iot.models.KafkaTopicName
import com.robotutor.iot.models.Message
import com.robotutor.loggingstarter.logOnError
import com.robotutor.loggingstarter.logOnSuccess
import com.robotutor.loggingstarter.serializer.DefaultSerializer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class KafkaPublisher(
    private val reactiveKafkaProducerTemplate: ReactiveKafkaProducerTemplate<String, String>,
) {
    fun publish(topicName: KafkaTopicName, key: String? = null, message: Message): Mono<Message> {
        val messageAsString = DefaultSerializer.serialize(message)
        return Mono.deferContextual { ctx ->
            val headers = ctx.stream()
                .map { (k, v) ->
                    RecordHeader(k.toString(), v.toString().toByteArray())
                }
                .toList()
            val producerRecord = ProducerRecord(topicName.toString(), null, key, messageAsString, headers)
            reactiveKafkaProducerTemplate.send(producerRecord)
                .map {
                    message
                }
        }
            .logOnSuccess("Successfully published to $topicName")
            .logOnError("", "Failed to publish to $topicName")
    }
}
