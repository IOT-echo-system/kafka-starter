package com.robotutor.iot

import com.robotutor.iot.models.AuditMessage
import com.robotutor.iot.models.AuditStatus
import com.robotutor.iot.models.KafkaTopicName
import com.robotutor.iot.services.KafkaPublisher
import com.robotutor.iot.utils.createMono
import com.robotutor.iot.utils.models.UserData
import reactor.core.publisher.Mono
import reactor.util.context.ContextView
import java.time.LocalDateTime
import java.time.ZoneId

fun <T : Any> Mono<T>.auditOnError(
    event: String,
    metadata: Map<String, Any?> = emptyMap(),
    userId: String? = null,
    deviceId: String? = null,
): Mono<T> {
    return onErrorResume { error ->
        Mono.deferContextual { ctx ->
            auditOnError<T>(ctx, userId, metadata, event, deviceId, error)
        }
    }
}

fun <T : Any> Mono<T>.auditOnSuccess(
    event: String,
    metadata: Map<String, Any?> = emptyMap(),
    userId: String? = null,
    deviceId: String? = null,
): Mono<T> {
    return flatMap { result ->
        Mono.deferContextual { ctx ->
            auditOnSuccess(ctx, userId, metadata, event, deviceId, result)
        }
    }
}

fun getDeviceId(contextView: ContextView): String? {
    return contextView.getOrDefault("deviceId", "missing-device-id")
}

fun getUserId(contextView: ContextView): String {
    try {
        val userData = contextView.get(UserData::class.java)
        return userData.userId
    } catch (ex: Exception) {
        return "missing-user-id"
    }
}

fun <T : Any> auditOnError(
    ctx: ContextView,
    userId: String?,
    metadata: Map<String, Any?>,
    event: String,
    deviceId: String?,
    error: Throwable
): Mono<T> {
    val kafkaPublisher = ctx.getOrEmpty<KafkaPublisher>(KafkaPublisher::class.java)
    return if (kafkaPublisher.isPresent) {
        val auditMessage = AuditMessage(
            status = AuditStatus.FAILURE,
            userId = userId ?: getUserId(ctx),
            metadata = metadata,
            event = event,
            deviceId = deviceId ?: getDeviceId(ctx),
            timestamp = LocalDateTime.now(ZoneId.of("UTC"))
        )
        kafkaPublisher.get().publish(KafkaTopicName.AUDIT, "audit", auditMessage)
            .flatMap { Mono.error(error) }
    } else {
        Mono.error(error)
    }
}

fun <T : Any> auditOnSuccess(
    ctx: ContextView,
    userId: String?,
    metadata: Map<String, Any?>,
    event: String,
    deviceId: String?,
    result: T
): Mono<T> {
    val kafkaPublisher = ctx.getOrEmpty<KafkaPublisher>(KafkaPublisher::class.java)
    return if (kafkaPublisher.isPresent) {
        val auditMessage = AuditMessage(
            status = AuditStatus.SUCCESS,
            userId = userId ?: getUserId(ctx),
            metadata = metadata,
            event = event,
            deviceId = deviceId ?: getDeviceId(ctx),
            timestamp = LocalDateTime.now(ZoneId.of("UTC"))
        )
        kafkaPublisher.get().publish(KafkaTopicName.AUDIT, "audit", auditMessage)
            .map { result }
    } else {
        createMono(result)
    }
}
