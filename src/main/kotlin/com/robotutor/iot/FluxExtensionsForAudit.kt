package com.robotutor.iot

import reactor.core.publisher.Flux

fun <T : Any> Flux<T>.auditOnError(
    event: String,
    metadata: Map<String, Any?> = emptyMap(),
    userId: String? = null,
    deviceId: String? = null,
): Flux<T> {
    return onErrorResume { error ->
        Flux.deferContextual { ctx ->
            auditOnError<T>(ctx, userId, metadata, event, deviceId, error)
        }
    }
}

fun <T : Any> Flux<T>.auditOnSuccess(
    event: String,
    metadata: Map<String, Any?> = emptyMap(),
    userId: String? = null,
    deviceId: String? = null,
): Flux<T> {
    return flatMap { result ->
        Flux.deferContextual { ctx ->
            auditOnSuccess<T>(ctx, userId, metadata, event, deviceId, result)
        }
    }
}

