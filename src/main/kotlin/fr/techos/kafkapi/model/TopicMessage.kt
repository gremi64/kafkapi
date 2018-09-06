package fr.techos.kafkapi.model

data class TopicMessage(val topic: String,
                        val group: String,
                        val partition: Int,
                        val offset: Long,
                        val timestamp: Long,
                        val key: String?,
                        val message: String?)