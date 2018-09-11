package fr.techos.kafkapi.model

data class CommitResult (val committedOffset: Long?,
                         val topic: String,
                         val group: String,
                         val partition: Int,
                         val previousOffset: Long?)

data class OffsetsResult (val position: Long,
                          val committed: Long?)

data class TopicGroupOffsetResult (val topic: String,
                                   val group: String,
                                   var partitionOffsetResult: List<PartitionOffsetResult>)

data class PartitionOffsetResult (val partition: Int,
                                  val offset: Long?)

data class TopicMessage(val topic: String,
                        val group: String,
                        val partition: Int,
                        val offset: Long,
                        val timestamp: Long,
                        val key: String?,
                        val message: String?)