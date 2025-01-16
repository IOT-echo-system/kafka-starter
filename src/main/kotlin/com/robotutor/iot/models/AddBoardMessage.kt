package com.robotutor.iot.models

data class AddBoardMessage(val premisesId: String, val boardId: String) : Message()
