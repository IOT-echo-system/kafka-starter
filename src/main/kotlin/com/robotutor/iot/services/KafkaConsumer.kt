package com.robotutor.iot.services

import com.robotutor.iot.models.KafkaTopicName
import com.robotutor.iot.models.Message
import com.robotutor.iot.utils.createMono
import com.robotutor.loggingstarter.Logger
import com.robotutor.loggingstarter.logOnError
import com.robotutor.loggingstarter.logOnSuccess
import com.robotutor.loggingstarter.serializer.DefaultSerializer
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import java.nio.charset.StandardCharsets

@Service
class KafkaConsumer(
    private val kafkaReceiverFactory: (List<String>) -> ReactiveKafkaConsumerTemplate<String, String>
) {
    val logger = Logger(this::class.java)

    fun <T : Message> consume(topics: List<KafkaTopicName>, messageType: Class<T>): Flux<KafkaTopicMessage<T>> {
        val kafkaReceiver = kafkaReceiverFactory(topics.map { it.toString() })
        return kafkaReceiver.receive()
            .flatMap { receiverRecord ->
                val message = DefaultSerializer.deserialize(receiverRecord.value(), messageType)
                val topic = DefaultSerializer.deserialize(receiverRecord.topic(), KafkaTopicName::class.java)
                createMono(KafkaTopicMessage(topic, message))
                    .contextWrite { ctx ->
                        val headers = receiverRecord.headers()
                            .associate { it.key() to String(it.value(), StandardCharsets.UTF_8) }
                        headers.entries.fold(ctx) { acc, (key, value) ->
                            acc.put(key, value)
                        }
                    }
                    .doFinally {
                        receiverRecord.receiverOffset().acknowledge()
                    }
                    .logOnSuccess(logger, "Successfully consumed kafka topic to $topic")
                    .logOnError(logger, "", "Failed to consume kafka topic to $topic")
            }
    }
}

data class KafkaTopicMessage<T : Message>(val topic: KafkaTopicName, val message: T)
