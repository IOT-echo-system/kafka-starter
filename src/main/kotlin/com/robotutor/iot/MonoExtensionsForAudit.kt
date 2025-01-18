package com.robotutor.iot

import com.robotutor.iot.models.AuditMessage
import com.robotutor.iot.models.AuditStatus
import com.robotutor.iot.models.KafkaTopicName
import com.robotutor.iot.services.KafkaPublisher
import com.robotutor.iot.utils.createMono
import com.robotutor.iot.utils.models.UserData
import com.robotutor.loggingstarter.models.ServerWebExchangeDTO
import reactor.core.publisher.Mono
import reactor.util.context.ContextView
import java.time.LocalDateTime
import java.time.ZoneId

fun <T : Any> Mono<T>.auditOnError(
    event: String,
    metadata: Map<String, Any?> = emptyMap(),
    userId: String? = null,
    premisesId: String? = null,
): Mono<T> {
    return onErrorResume { error ->
        Mono.deferContextual { ctx ->
            auditOnError<T>(ctx, userId, metadata, event, premisesId, error)
        }
    }
}

fun <T : Any> Mono<T>.auditOnSuccess(
    event: String,
    metadata: Map<String, Any?> = emptyMap(),
    userId: String? = null,
    premisesId: String? = null,
): Mono<T> {
    return flatMap { result ->
        Mono.deferContextual { ctx ->
            auditOnSuccess(ctx, userId, metadata, event, premisesId, result)
        }
    }
}

fun getPremisesId(contextView: ContextView): String {
    try {
        val exchangeDTO = contextView.get(ServerWebExchangeDTO::class.java)
        return exchangeDTO.requestDetails.headers["x-premises-id"]?.first() ?: "missing-premises-id"
    } catch (ex: Exception) {
        return "missing-premises-id"
    }
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
    premisesId: String?,
    error: Throwable
): Mono<T> {
    val kafkaPublisher = ctx.getOrEmpty<KafkaPublisher>(KafkaPublisher::class.java)
    return if (kafkaPublisher.isPresent) {
        val auditMessage = AuditMessage(
            status = AuditStatus.FAILURE,
            userId = userId ?: getUserId(ctx),
            metadata = metadata,
            event = event,
            premisesId = premisesId ?: getPremisesId(ctx),
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
    premisesId: String?,
    result: T
): Mono<T> {
    val kafkaPublisher = ctx.getOrEmpty<KafkaPublisher>(KafkaPublisher::class.java)
    return if (kafkaPublisher.isPresent) {
        val auditMessage = AuditMessage(
            status = AuditStatus.SUCCESS,
            userId = userId ?: getUserId(ctx),
            metadata = metadata,
            event = event,
            premisesId = premisesId ?: getPremisesId(ctx),
            timestamp = LocalDateTime.now(ZoneId.of("UTC"))
        )
        kafkaPublisher.get().publish(KafkaTopicName.AUDIT, "audit", auditMessage)
            .map { result }
    } else {
        createMono(result)
    }
}
