package com.robotutor.iot.models

import java.time.LocalDateTime
import java.time.ZoneId

data class AuditMessage(
    val status: AuditStatus,
    val userId: String,
    val metadata: Map<String, Any?>,
    val event: String,
    val accountId: String? = null,
    val premisesId: String? = null,
    val timestamp: LocalDateTime = LocalDateTime.now(ZoneId.of("UTC"))
) : Message()

enum class AuditStatus {
    SUCCESS,
    FAILURE
}

