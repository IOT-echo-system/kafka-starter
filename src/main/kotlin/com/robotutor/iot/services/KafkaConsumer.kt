package com.robotutor.iot.services

import com.robotutor.iot.models.KafkaTopicName
import com.robotutor.iot.models.Message
import com.robotutor.loggingstarter.serializer.DefaultSerializer
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.nio.charset.StandardCharsets

@Service
class KafkaConsumer(
    private val kafkaReceiverFactory: (List<String>) -> ReactiveKafkaConsumerTemplate<String, String>
) {
    fun <T : Message> consume(
        topics: List<KafkaTopicName>,
        messageType: Class<T>,
        onMessage: (key: String, value: T) -> Mono<T>,
    ): Flux<T> {
        val kafkaReceiver = kafkaReceiverFactory(topics.map { it.toString() })
        return kafkaReceiver.receive()
            .flatMap { receiverRecord ->
                val value = DefaultSerializer.deserialize(receiverRecord.value(), messageType)
                Mono.deferContextual {
                    onMessage(receiverRecord.key(), value)
                }
                    .contextWrite { ctx ->
                        val headers = receiverRecord.headers()
                            .associate { it.key() to String(it.value(), StandardCharsets.UTF_8) }
                        headers.entries.fold(ctx) { acc, (key, value) ->
                            acc.put(key, value)
                        }
                    }
                    .doOnSuccess {
                        receiverRecord.receiverOffset().acknowledge()
                    }
            }
    }
}
