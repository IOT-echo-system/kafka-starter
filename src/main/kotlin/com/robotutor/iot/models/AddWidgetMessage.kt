package com.robotutor.iot.models

data class AddWidgetMessage(
    val widgetId: String,
    val zoneId: String,
) : Message()


