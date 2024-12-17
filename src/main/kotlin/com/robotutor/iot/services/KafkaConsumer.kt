package com.robotutor.iot.services

import com.robotutor.iot.models.Message
import com.robotutor.loggingstarter.serializer.DefaultSerializer
import org.springframework.stereotype.Service
import reactor.core.Disposable
import reactor.kafka.receiver.KafkaReceiver

@Service
class KafkaConsumer(
    private val kafkaReceiverFactory: (List<String>) -> KafkaReceiver<String, String>
) {

    fun <T : Message> consume(
        topics: List<String>,
        messageType: Class<T>,
        onMessage: (key: String, value: T) -> T,
    ): Disposable {
        val kafkaReceiver = kafkaReceiverFactory(topics)

        return kafkaReceiver.receive()
            .map {
                it.receiverOffset().acknowledge()
                val value = DefaultSerializer.deserialize(it.value(), messageType)
                onMessage(it.key(), value)
            }
            .subscribe()
    }
}
