package com.robotutor.iot.services

import com.robotutor.iot.models.KafkaTopicName
import com.robotutor.iot.models.Message
import com.robotutor.loggingstarter.Logger
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
    val logger = Logger(this::class.java)
    fun publish(topicName: KafkaTopicName, key: String? = null, message: Message): Mono<Message> {
        val messageAsString = DefaultSerializer.serialize(message)
        return Mono.deferContextual { ctx ->
            val headers = ctx.stream()
                .map { (k, v) ->
                    RecordHeader(k.toString(), DefaultSerializer.serialize(v).toByteArray())
                }
                .toList()
            val producerRecord = ProducerRecord(topicName.toString(), null, key, messageAsString, headers)
            reactiveKafkaProducerTemplate.send(producerRecord).map { message }
        }
            .logOnSuccess(logger, "Successfully published kafka topic to $topicName")
            .logOnError(logger, "", "Failed to publish kafka topic to $topicName")
    }
}
