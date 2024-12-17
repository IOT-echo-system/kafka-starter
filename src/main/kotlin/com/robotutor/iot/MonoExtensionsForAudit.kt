package com.robotutor.iot

import com.robotutor.iot.models.AuditEvent
import com.robotutor.iot.models.AuditMessage
import com.robotutor.iot.models.AuditStatus
import com.robotutor.iot.models.KafkaTopicName
import com.robotutor.iot.services.KafkaPublisher
import com.robotutor.iot.utils.models.UserAuthenticationData
import reactor.core.publisher.Mono
import reactor.util.context.ContextView
import java.time.LocalDateTime
import java.time.ZoneId

fun <T> Mono<T>.auditOnError(
    event: AuditEvent,
    metadata: Map<String, Any> = emptyMap(),
    userId: String? = null,
    deviceId: String? = null,
): Mono<T> {
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


fun <T> Mono<T>.auditOnSuccess(
    event: AuditEvent,
    metadata: Map<String, Any> = emptyMap(),
    userId: String? = null,
    accountId: String? = null,
    deviceId: String? = null,
): Mono<T> {
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
        val userAuthenticationData = contextView.get(UserAuthenticationData::class.java)
        return userAuthenticationData.userId
    } catch (ex: Exception) {
        return "missing-user-id"
    }
}
