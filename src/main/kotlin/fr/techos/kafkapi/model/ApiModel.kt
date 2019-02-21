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
                                  val minOffset: Long?,
                                  val offset: Long?,
                                  val maxOffset: Long?)

data class TopicMessage(val topic: String,
                        val group: String,
                        val partition: Int,
                        val offset: Long,
                        val timestamp: Long,
                        val key: String?,
                        val message: String?,
                        var avroSchemaId: Int?)

data class KafkaConsumerConfigs(val brokers : List<Pair<String, String>>,
                                val securityOptions : List<Pair<String, String>>)