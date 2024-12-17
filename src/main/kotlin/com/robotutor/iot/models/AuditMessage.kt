package com.robotutor.iot.models

import java.time.LocalDateTime
import java.time.ZoneId

data class AuditMessage(
    val status: AuditStatus,
    val userId: String,
    val metadata: Map<String, Any>,
    val event: AuditEvent,
    val accountId: String? = null,
    val deviceId: String? = null,
    val timestamp: LocalDateTime = LocalDateTime.now(ZoneId.of("UTC"))
) : Message()

enum class AuditStatus {
    SUCCESS,
    FAILURE
}

enum class AuditEvent {
    SIGN_UP,
    VERIFY_PASSWORD,
    LOGIN,
    GENERATE_TOKEN,
    GENERATE_OTP,
    VERIFY_OTP,
    RESET_PASSWORD,
    SEND_EMAIL,
    CREATE_ACCOUNT,
    LOG_OUT,
    CREATE_BOARD,
    UPDATE_BOARD,
}
