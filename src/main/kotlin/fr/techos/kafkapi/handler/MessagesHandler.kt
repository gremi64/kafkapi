package fr.techos.kafkapi.handler

import fr.techos.kafkapi.model.OffsetsResult
import fr.techos.kafkapi.model.TopicMessage
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.BodyInserters.fromObject
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.ok
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import java.time.Duration
import java.util.*
import kotlin.math.min

@Component
class MessagesHandler {
    private val logger = KotlinLogging.logger {}

    @Autowired
    lateinit var kafkaConsumerConfig: Properties

    /**
     * Renvoie la liste de tous les messages d'un topic, toutes partitions confondues
     */
    fun messagesForTopic(request: ServerRequest): Mono<ServerResponse> {
        val topic: String = request.pathVariable("topic")
        val group: String = request.queryParam("group").orElse("myGroup")

        val kafkaConsumer = getKafkaConsumer(group)
        val results = mutableMapOf<Int, List<TopicMessage>>()

        kafkaConsumer.partitionsFor(topic).forEach {
            logger.info("Processing partition ${it.partition()}")
            // Assignation de la partition
            val topicPartition = TopicPartition(topic, it.partition())
            kafkaConsumer.assign(mutableListOf(topicPartition))
            results[it.partition()] = pollMessages(kafkaConsumer, topic, group)
            logger.info("End of work for partition ${it.partition()}")
        }

        closeConsumer(kafkaConsumer)

        return ok().body(BodyInserters.fromObject(results)).toMono()
    }

    /**
     * Renvoie les messages d'un topic, sur UNE seule partition, à partir d'un offset donné
     */
    fun messagesForPartition(request: ServerRequest): Mono<ServerResponse> {
        val topic: String = request.pathVariable("topic")
        val partition: Int = request.pathVariable("partition").toInt()
        val offset: Long = request.queryParam("offset").orElse("-2").toLong()
        val group: String = request.queryParam("group").orElse("myGroup")
        val limit: Int = request.queryParam("limit").orElse("10").toInt()

        val kafkaConsumer = getKafkaConsumer(group)

        // Assignation de la partition qui nous intéresse
        val topicPartition = TopicPartition(topic, partition)
        kafkaConsumer.assign(mutableListOf(topicPartition))
        val oldOffsetsInformation = setOffset(kafkaConsumer, topicPartition, offset)
        logger.info("Partition ${topicPartition.partition()} : Current offset is ${oldOffsetsInformation.position} " +
                "Committed offset is ->${oldOffsetsInformation.committed}")

        val polled = pollMessages(kafkaConsumer, topic, group)

        // Messages
        return ok().body(fromObject(polled.subList(0, min(polled.size, limit))))
    }

    private fun pollMessages(kafkaConsumer: KafkaConsumer<String, String>, topic: String, group: String): MutableList<TopicMessage> {
        val partResult = mutableListOf<TopicMessage>()
        var workToDo = true
        while (workToDo) {
            val polled = kafkaConsumer.poll(Duration.ofMillis(200))
            polled.forEach {
                partResult += TopicMessage(topic, group, it.partition(), it.offset(), it.timestamp(), it.key(), it.value())
            }
            workToDo = !polled.isEmpty
        }
        return partResult
    }

    private fun closeConsumer(kafkaConsumer: KafkaConsumer<String, String>) {
        logger.info("Closing KafkaConsumer")
        kafkaConsumer.close(Duration.ofSeconds(10))
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