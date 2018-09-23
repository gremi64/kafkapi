package fr.techos.kafkapi.helper

import fr.techos.kafkapi.model.OffsetsResult
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

class KafkaConsumerHelper {
    companion object {
        private val logger = KotlinLogging.logger {}

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