package fr.techos.kafkapi.handler

import fr.techos.kafkapi.model.CommitResult
import fr.techos.kafkapi.model.OffsetsResult
import fr.techos.kafkapi.model.PartitionOffsetResult
import fr.techos.kafkapi.model.TopicGroupOffsetResult
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.reactive.function.BodyInserters.fromObject
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.ok
import org.springframework.web.reactive.function.server.body
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import java.time.Duration
import java.util.*

@Component
class OffsetsHandler {

    private val logger = KotlinLogging.logger {}

    @Autowired
    lateinit var kafkaConsumerConfig: Properties

    /**
     * Commit l'offset d'un topic, pou UN groupe, sur UNE seule partition, à partir d'UN offset donné
     */
    fun commitForPartition(request: ServerRequest): Mono<ServerResponse> {

        val topic: String = request.pathVariable("topic")
        val partition: Int = request.pathVariable("partition").toInt()
        val group: String = request.queryParam("group").orElse("myGroup")
        val offset: Long = request.queryParam("offset").orElse("-2").toLong()

        val kafkaConsumer = getKafkaConsumer(group)

        // Assignation de la partition qui nous intéresse
        val topicPartition = TopicPartition(topic, partition)
        kafkaConsumer.assign(mutableListOf(topicPartition))
        val oldOffsetsInformation = setOffset(kafkaConsumer, topicPartition, offset)
        logger.info("Partition ${topicPartition.partition()} : Current offset is now ${oldOffsetsInformation.position}. " +
                "Committed offset is still ->${oldOffsetsInformation.committed}")
        kafkaConsumer.commitSync(mutableMapOf(Pair(topicPartition, OffsetAndMetadata(oldOffsetsInformation.position, ""))))
        val newCommittedOffset = kafkaConsumer.committed(topicPartition)?.offset()
        logger.info("Partition ${topicPartition.partition()} : Current committed offset is now ->$newCommittedOffset")

        return ok().body(fromObject(CommitResult(newCommittedOffset, topic, group, partition, oldOffsetsInformation.committed))).toMono()
    }

    /**
     * Renvoie la liste de tous les messages d'un topic, toutes partitions confondues
     */
    fun offsetForTopic(request: ServerRequest): Mono<ServerResponse> {
        val topic: String = request.pathVariable("topic")
        val group: String = request.queryParam("group").orElse("myGroup")

        val kafkaConsumer = getKafkaConsumer(group)
        val partitionOffsetResult = mutableListOf<PartitionOffsetResult>()
        val results = TopicGroupOffsetResult(topic, group, partitionOffsetResult)

        kafkaConsumer.partitionsFor(topic)?.forEach {
            logger.info("Processing partition ${it.partition()}")
            // Assignation de la partition
            val topicPartition = TopicPartition(topic, it.partition())
            kafkaConsumer.assign(mutableListOf(topicPartition))
            partitionOffsetResult.add(offsetsInfos(kafkaConsumer, topicPartition))
            logger.info("End of work for partition ${it.partition()}")
        }

        // Tri du tableau par partition
        partitionOffsetResult.sortBy {
            it.partition
        }

        closeConsumer(kafkaConsumer)

        return ok().body(fromObject(results)).toMono()
    }

    private fun closeConsumer(kafkaConsumer: KafkaConsumer<String, String>) {
        logger.info("Closing KafkaConsumer")
        kafkaConsumer.close(Duration.ofSeconds(10))
    }

    private fun offsetsInfos(kafkaConsumer: KafkaConsumer<String, String>, topicPartition: TopicPartition): PartitionOffsetResult {
        val partitionNumber = topicPartition.partition()
        val currentOffset = kafkaConsumer.committed(topicPartition)?.offset()
        val minOffset = kafkaConsumer.beginningOffsets(mutableListOf(topicPartition))?.get(topicPartition)
        val maxOffset = kafkaConsumer.endOffsets(mutableListOf(topicPartition))?.get(topicPartition)

        return PartitionOffsetResult(partitionNumber, minOffset, currentOffset, maxOffset)
    }

    private fun getKafkaConsumer(group: String): KafkaConsumer<String, String> {
        // Configuration
        kafkaConsumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = group

        // Création du consumer avec la config à jour
        return KafkaConsumer(kafkaConsumerConfig)
    }

    /**
     * Renvoie l'offset commité ainsi que l'offset courant (après modification éventuelle)
     */
    private fun setOffset(kafkaConsumer: KafkaConsumer<String, String>, topicPartition: TopicPartition, offset: Long): OffsetsResult {
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