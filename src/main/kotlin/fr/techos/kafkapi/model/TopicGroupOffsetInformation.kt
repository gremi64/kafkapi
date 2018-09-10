package fr.techos.kafkapi.model

data class TopicGroupOffsetInformation (val topic: String,
                                        val group: String,
                                        var partitionOffsetInformation: List<PartitionOffsetInformation>) {

    data class PartitionOffsetInformation (val partition: Int,
                                           val offset: Long?)

}