package fr.techos.kafkapi.handler

import fr.techos.kafkapi.config.KafkaConfig
import fr.techos.kafkapi.helper.KafkaConsumerHelper
import fr.techos.kafkapi.model.TopicMessage
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.BodyInserters.fromObject
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.ok
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import java.time.Duration
import kotlin.math.min

@Component
class MessagesHandler(val kafkaConfig: KafkaConfig) {

    private val logger = KotlinLogging.logger {}

    /**
     * Renvoie la liste de tous les messages d'un topic, toutes partitions confondues
     */
    fun messagesForTopic(request: ServerRequest): Mono<ServerResponse> {
        val topic: String = request.pathVariable("topic")
        val group: String = request.queryParam("group").orElse("myGroup")

        val kafkaConsumerConfig = kafkaConfig.getKafkaConsumerConfig()

        if (group.isEmpty()) {
            kafkaConsumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = "myGroup"
        } else {
            kafkaConsumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = group
        }

        val kafkaConsumer = KafkaConsumer<String, String>(kafkaConsumerConfig)

        val results = mutableMapOf<Int, List<TopicMessage>>()

        kafkaConsumer.partitionsFor(topic).forEach {
            logger.info("Processing partition ${it.partition()}")
            // Assignation de la partition
            val topicPartition = TopicPartition(topic, it.partition())
            kafkaConsumer.assign(mutableListOf(topicPartition))
            results[it.partition()] = pollMessages(kafkaConsumer, topic, group)
            logger.info("End of work for partition ${it.partition()}")
        }

        logger.info("Closing KafkaConsumer")
        kafkaConsumer.close(Duration.ofSeconds(10))

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

        val kafkaConsumerConfig = kafkaConfig.getKafkaConsumerConfig()

        if (group.isEmpty()) {
            kafkaConsumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = "myGroup"
        } else {
            kafkaConsumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = group
        }

        val kafkaConsumer = KafkaConsumer<String, String>(kafkaConsumerConfig)

        // Assignation de la partition qui nous intéresse
        val topicPartition = TopicPartition(topic, partition)
        kafkaConsumer.assign(mutableListOf(topicPartition))
        val oldOffsetsInformation = KafkaConsumerHelper.setOffset(kafkaConsumer, topicPartition, offset)
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
            val polled = kafkaConsumer.poll(Duration.ofMillis(400))
            polled.forEach {
                partResult += TopicMessage(topic, group, it.partition(), it.offset(), it.timestamp(), it.key(), it.value())
            }
            workToDo = !polled.isEmpty
        }
        return partResult
    }
}