package com.robotutor.iot

import com.robotutor.iot.models.AuditMessage
import com.robotutor.iot.models.AuditStatus
import com.robotutor.iot.models.KafkaTopicName
import com.robotutor.iot.services.KafkaPublisher
import com.robotutor.iot.utils.models.UserData
import reactor.core.publisher.Flux
import reactor.util.context.ContextView
import java.time.LocalDateTime
import java.time.ZoneId

fun <T> Flux<T>.auditOnError(
    event: String,
    metadata: Map<String, Any> = emptyMap(),
    userId: String? = null,
    deviceId: String? = null,
): Flux<T> {
    return doOnEach { signal ->
        if (signal.isOnError) {
            val kafkaPublisher = signal.contextView.get(KafkaPublisher::class.java)
            val auditMessage = AuditMessage(
                status = AuditStatus.FAILURE,
                userId = userId ?: getUserId(signal.contextView),
                metadata = metadata,
                event = event,
                deviceId = deviceId ?: getDeviceId(signal.contextView),
                timestamp = LocalDateTime.now(ZoneId.of("UTC"))
            )
            kafkaPublisher.publish(KafkaTopicName.AUDIT, "audit", auditMessage)
        }
    }
}


fun <T> Flux<T>.auditOnSuccess(
    event: String,
    metadata: Map<String, Any> = emptyMap(),
    userId: String? = null,
    deviceId: String? = null,
): Flux<T> {
    return doOnEach { signal ->
        if (signal.isOnNext) {
            val kafkaPublisher = signal.contextView.get(KafkaPublisher::class.java)
            val auditMessage = AuditMessage(
                status = AuditStatus.SUCCESS,
                userId = userId ?: getUserId(signal.contextView),
                metadata = metadata,
                event = event,
                deviceId = deviceId ?: getDeviceId(signal.contextView),
                timestamp = LocalDateTime.now(ZoneId.of("UTC"))
            )
            kafkaPublisher.publish(KafkaTopicName.AUDIT, "audit", auditMessage)
        }
    }
}

private fun getDeviceId(contextView: ContextView): String? {
    return contextView.getOrDefault("deviceId", "missing-device-id")
}

private fun getUserId(contextView: ContextView): String {
    try {
        val userData = contextView.get(UserData::class.java)
        return userData.userId
    } catch (ex: Exception) {
        return "missing-user-id"
    }
}
