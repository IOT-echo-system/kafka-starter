package com.robotutor.iot.services

import com.robotutor.iot.models.KafkaTopicName
import com.robotutor.iot.models.Message
import com.robotutor.iot.utils.models.PremisesData
import com.robotutor.iot.utils.models.UserData
import com.robotutor.loggingstarter.Logger
import com.robotutor.loggingstarter.logOnError
import com.robotutor.loggingstarter.logOnSuccess
import com.robotutor.loggingstarter.models.ServerWebExchangeDTO
import com.robotutor.loggingstarter.serializer.DefaultSerializer
import org.apache.kafka.common.header.Headers
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.context.Context
import java.nio.charset.StandardCharsets

@Service
class KafkaConsumer(
    private val kafkaReceiverFactory: (List<String>) -> ReactiveKafkaConsumerTemplate<String, String>
) {
    val logger = Logger(this::class.java)

    fun <T : Message, R : Any> consume(
        topics: List<KafkaTopicName>,
        messageType: Class<T>,
        process: (it: KafkaTopicMessage<T>) -> Mono<R>
    ): Flux<R> {
        val kafkaReceiver = kafkaReceiverFactory(topics.map { it.toString() })
        return kafkaReceiver.receive()
            .flatMap { receiverRecord ->
                val message = DefaultSerializer.deserialize(receiverRecord.value(), messageType)
                val topic = DefaultSerializer.deserialize(receiverRecord.topic(), KafkaTopicName::class.java)
                Mono.just(KafkaTopicMessage(topic, message))
                    .flatMap {
                        process(it)
                    }
                    .contextWrite { ctx -> writeContext(receiverRecord.headers(), ctx) }
                    .doFinally { receiverRecord.receiverOffset().acknowledge() }
                    .logOnSuccess(logger, "Successfully consumed kafka topic to $topic")
                    .logOnError(logger, "", "Failed to consume kafka topic to $topic")
            }
    }

    private fun writeContext(receiverHeaders: Headers, ctx: Context): Context {
        val headers = receiverHeaders.toArray()
            .map { KafkaHeader(it.key(), it.value().toString(StandardCharsets.UTF_8)) }
        val userData = headers.find { it.key == "userData" }?.value
        val premisesData = headers.find { it.key == "premisesData" }?.value
        val exchangeDTO = headers.find { it.key == "exchange" }?.value
        var newCtx = ctx
        userData?.let {
            newCtx = newCtx.put(UserData::class.java, DefaultSerializer.deserialize(userData, UserData::class.java))
        }
        premisesData?.let {
            newCtx = newCtx.put(
                PremisesData::class.java,
                DefaultSerializer.deserialize(premisesData, PremisesData::class.java)
            )
        }
        exchangeDTO?.let {
            newCtx = newCtx.put(
                ServerWebExchangeDTO::class.java,
                DefaultSerializer.deserialize(exchangeDTO, ServerWebExchangeDTO::class.java)
            )
        }
        return newCtx
    }
}

data class KafkaTopicMessage<T : Message>(val topic: KafkaTopicName, val message: T)
data class KafkaHeader(val key: String, val value: String)
