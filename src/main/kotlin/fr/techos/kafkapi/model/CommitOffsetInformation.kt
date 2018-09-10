package fr.techos.kafkapi.model

data class CommitOffsetInformation (val committedOffset: Long?,
                                    val topic: String,
                                    val group: String,
                                    val partition: Int,
                                    val previousOffset: Long?)