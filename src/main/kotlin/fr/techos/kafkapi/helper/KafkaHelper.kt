package fr.techos.kafkapi.helper

import fr.techos.kafkapi.model.CommitResult
import fr.techos.kafkapi.model.OffsetsResult
import fr.techos.kafkapi.model.PartitionOffsetResult
import fr.techos.kafkapi.model.TopicGroupOffsetResult
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.*

class KafkaConsumerHelper {
    companion object {
        private val logger = KotlinLogging.logger {}

        fun getTopicOffsetsForGroup(topic: String, group: String, kafkaConsumerConfig: Properties): TopicGroupOffsetResult {
            val kafkaConsumer = KafkaConsumer<String, String>(kafkaConsumerConfig)

            val partitionOffsetResult = mutableListOf<PartitionOffsetResult>()

            kafkaConsumer.partitionsFor(topic)?.forEach {
                logger.info("Processing partition ${it.partition()}")

                val topicPartition = TopicPartition(topic, it.partition())
                kafkaConsumer.assign(mutableListOf(topicPartition))

                partitionOffsetResult.add(
                        PartitionOffsetResult(
                                topicPartition.partition(),
                                kafkaConsumer.beginningOffsets(mutableListOf(topicPartition))?.get(topicPartition),
                                kafkaConsumer.committed(topicPartition)?.offset(),
                                kafkaConsumer.endOffsets(mutableListOf(topicPartition))?.get(topicPartition)))
                logger.info("End of work for partition ${it.partition()}")
            }

            logger.info("Closing KafkaConsumer")
            kafkaConsumer.close(Duration.ofSeconds(10))

            // Tri du tableau par partition
            partitionOffsetResult.sortBy {
                it.partition
            }

            return TopicGroupOffsetResult(topic, group, partitionOffsetResult)
        }

        /**
         * 1- Assign to given partition
         * 2- Moving to given offset on partition
         * 3- Commit the new offset
         * 4- Retrieve the new offset
         */
        fun commitResult(kafkaConsumerConfig: Properties, topic: String, partition: Int, group: String, offset: Long): CommitResult {
            val topicPartition = TopicPartition(topic, partition)

            val kafkaConsumer = KafkaConsumer<String, String>(kafkaConsumerConfig)
            kafkaConsumer.assign(mutableListOf(topicPartition))

            val oldOffsetsInformation = this.setOffset(kafkaConsumer, topicPartition, offset)
            logger.info("Partition ${topicPartition.partition()} : Current offset is now ${oldOffsetsInformation.position}. " +
                    "Committed offset is still ->${oldOffsetsInformation.committed}")

            kafkaConsumer.commitSync(mutableMapOf(Pair(topicPartition, OffsetAndMetadata(oldOffsetsInformation.position, ""))))
            val newCommittedOffset = kafkaConsumer.committed(topicPartition)?.offset()
            logger.info("Partition ${topicPartition.partition()} : Current committed offset is now ->$newCommittedOffset")

            return CommitResult(newCommittedOffset, topic, group, partition, oldOffsetsInformation.committed)
        }

        /**
         * Renvoie l'offset commité ainsi que l'offset courant (après modification éventuelle)
         */
        fun setOffset(kafkaConsumer: KafkaConsumer<String, String>, topicPartition: TopicPartition, offset: Long): OffsetsResult {
            val committed = kafkaConsumer.committed(topicPartition)

            when (offset) {
                -2L -> {
                    logger.info("Leaving offset alone")
                    // Si on ne souhaite pas modifier l'offset (= -2) et qu'on a jamais lu ce topic-groupe-partition
                    // On choisi de dire qu'on commence au début et non à la fin (=mode latest arrangé)
                    if (committed == null) {
                        logger.info("Setting offset to beginning even if latest mode is active")
                        kafkaConsumer.seekToBeginning(mutableListOf(topicPartition))
                    }
                }
                0L -> {
                    logger.info("Setting offset to begining")
                    kafkaConsumer.seekToBeginning(mutableListOf(topicPartition))
                }
                -1L -> {
                    logger.info("Setting it to the end")
                    kafkaConsumer.seekToEnd(mutableListOf(topicPartition))
                }
                else -> {
                    logger.info("Resetting offset to $offset")
                    kafkaConsumer.seek(topicPartition, offset)
                }
            }
            return OffsetsResult(kafkaConsumer.position(topicPartition), committed?.offset())
        }
    }
}