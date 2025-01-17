package com.robotutor.iot.services

import com.robotutor.iot.models.KafkaTopicName
import com.robotutor.iot.models.Message
import com.robotutor.iot.utils.models.PremisesData
import com.robotutor.iot.utils.models.UserData
import com.robotutor.loggingstarter.Logger
import com.robotutor.loggingstarter.logOnError
import com.robotutor.loggingstarter.logOnSuccess
import com.robotutor.loggingstarter.serializer.DefaultSerializer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import org.springframework.stereotype.Service
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono
import reactor.util.context.ContextView

@Service
class KafkaPublisher(
    private val reactiveKafkaProducerTemplate: ReactiveKafkaProducerTemplate<String, String>,
) {
    val logger = Logger(this::class.java)
    fun publish(topicName: KafkaTopicName, key: String? = null, message: Message): Mono<Message> {
        val messageAsString = DefaultSerializer.serialize(message)
        return Mono.deferContextual { ctx ->
            val headers = createHeadersRecord(ctx)
            val producerRecord = ProducerRecord(topicName.toString(), null, key, messageAsString, headers)
            reactiveKafkaProducerTemplate.send(producerRecord).map { message }
        }
            .logOnSuccess(logger, "Successfully published kafka topic to $topicName")
            .logOnError(logger, "", "Failed to publish kafka topic to $topicName")
    }

    private fun createHeadersRecord(ctx: ContextView): MutableList<RecordHeader> {
        val userData = ctx.get(UserData::class.java)
        val exchange = ctx.get(ServerWebExchange::class.java)
        val premisesData = ctx.getOrEmpty<PremisesData>(PremisesData::class.java)

        val headers = mutableListOf<RecordHeader>()
        headers.add(RecordHeader("userData", DefaultSerializer.serialize(userData).toByteArray()))
        headers.add(RecordHeader("exchange", DefaultSerializer.serialize(exchange).toByteArray()))
        if (premisesData.isPresent) {
            headers.add(RecordHeader("premisesData", DefaultSerializer.serialize(premisesData.get()).toByteArray()))
        }
        return headers
    }
}
