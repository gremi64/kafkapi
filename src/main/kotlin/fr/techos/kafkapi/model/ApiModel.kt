package fr.techos.kafkapi.model

data class CommitOffsetInformation (val committedOffset: Long?,
                                    val topic: String,
                                    val group: String,
                                    val partition: Int,
                                    val previousOffset: Long?)

data class OffsetsInformation (val position: Long,
                               val committed: Long?)

data class TopicGroupOffsetInformation (val topic: String,
                                        val group: String,
                                        var partitionOffsetInformation: List<PartitionOffsetInformation>) {

    data class PartitionOffsetInformation (val partition: Int,
                                           val offset: Long?)

}

data class TopicMessage(val topic: String,
                        val group: String,
                        val partition: Int,
                        val offset: Long,
                        val timestamp: Long,
                        val key: String?,
                        val message: String?)