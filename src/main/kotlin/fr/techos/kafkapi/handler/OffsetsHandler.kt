package fr.techos.kafkapi.handler

import fr.techos.kafkapi.config.KafkaConfig
import fr.techos.kafkapi.config.KafkaEnvProperties
import fr.techos.kafkapi.config.KafkaSecurityProperties
import fr.techos.kafkapi.helper.KafkaConsumerHelper
import fr.techos.kafkapi.model.CommitResult
import fr.techos.kafkapi.model.PartitionOffsetResult
import fr.techos.kafkapi.model.TopicGroupOffsetResult
import mu.KotlinLogging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SslConfigs
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters.fromObject
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.ok
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import java.time.Duration

@Component
class OffsetsHandler(val kafkaConfig: KafkaConfig,
                     val kafkaSecurityProperties: KafkaSecurityProperties,
                     val kafkaEnvProperties: KafkaEnvProperties) {

    private val logger = KotlinLogging.logger {}

    /**
     * Commit l'offset d'un topic, pou UN groupe, sur UNE seule partition, à partir d'UN offset donné
     */
    fun commitForPartition(request: ServerRequest): Mono<ServerResponse> {

        val topic: String = request.pathVariable("topic")
        val partition: Int = request.pathVariable("partition").toInt()
        val group: String = request.queryParam("group").orElse("myGroup")
        val offset: Long = request.queryParam("offset").orElse("-2").toLong()

        val kafkaConsumerConfig = kafkaConfig.getKafkaConsumerConfig()

        kafkaConsumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = group
        val kafkaConsumer = KafkaConsumer<String, String>(kafkaConsumerConfig)

        // Assignation de la partition qui nous intéresse
        val topicPartition = TopicPartition(topic, partition)
        kafkaConsumer.assign(mutableListOf(topicPartition))
        val oldOffsetsInformation = KafkaConsumerHelper.setOffset(kafkaConsumer, topicPartition, offset)
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
        val brokers: String? = request.queryParam("brokers").orElse(null)
        val security: String? = request.queryParam("security").orElse(null)

        val kafkaConsumerConfig = kafkaConfig.getKafkaConsumerConfig()

        kafkaConsumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = group

        if (!brokers.isNullOrEmpty()) {
            brokers.let { brokersKey ->
                this.kafkaEnvProperties.available[brokersKey]?.bootstrapServers.let {
                    kafkaConsumerConfig[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = it
                }
            }
        }

        if (!security.isNullOrEmpty()) {
            security.let {securityKey ->
                val prop = this.kafkaSecurityProperties.user[securityKey]
                prop?.let {
                    kafkaConsumerConfig[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = it.securityProtocol
                    kafkaConsumerConfig[SslConfigs.SSL_PROTOCOL_CONFIG] = it.securityProtocol
                    if (it.securityProtocol == "SSL") {
                        kafkaConsumerConfig[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = it.keyPassword
                        kafkaConsumerConfig[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] = it.keystoreLocation
                        kafkaConsumerConfig[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] = it.keystorePassword
                        kafkaConsumerConfig[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] = it.truststoreLocation
                        kafkaConsumerConfig[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] = it.truststorePassword
                    }
                }
            }
        }

        val kafkaConsumer = KafkaConsumer<String, String>(kafkaConsumerConfig)

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

        logger.info("Closing KafkaConsumer")
        kafkaConsumer.close(Duration.ofSeconds(10))

        return ok().body(fromObject(results)).toMono()
    }

    private fun offsetsInfos(kafkaConsumer: KafkaConsumer<String, String>, topicPartition: TopicPartition): PartitionOffsetResult {
        val partitionNumber = topicPartition.partition()
        val currentOffset = kafkaConsumer.committed(topicPartition)?.offset()
        val minOffset = kafkaConsumer.beginningOffsets(mutableListOf(topicPartition))?.get(topicPartition)
        val maxOffset = kafkaConsumer.endOffsets(mutableListOf(topicPartition))?.get(topicPartition)

        return PartitionOffsetResult(partitionNumber, minOffset, currentOffset, maxOffset)
    }
}