package fr.techos.kafkapi.controller


import fr.techos.kafkapi.model.CommitOffsetInformation
import fr.techos.kafkapi.model.OffsetsInformation
import fr.techos.kafkapi.model.TopicGroupOffsetInformation
import fr.techos.kafkapi.model.TopicMessage
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*
import java.time.Duration
import java.util.*
import java.util.logging.Logger
import kotlin.math.min


@RestController
class KafkaController {
    val log: Logger = Logger.getLogger("KafkaController")

    @Autowired
    lateinit var kafkaConsumerConfig: Properties

    /**
     * Renvoie la liste de tous les messages d'un topic, toutes partitions confondues
     */
    @GetMapping("/messages/{topic}")
    fun messagesForTopic(@PathVariable(value = "topic") topic: String,
                         @RequestParam(value = "groupId", defaultValue = "myGroup") groupId: String): MutableMap<Int, List<TopicMessage>> {

        val kafkaConsumer = getKafkaConsumer(groupId)
        val results = mutableMapOf<Int, List<TopicMessage>>()

        kafkaConsumer.partitionsFor(topic).forEach {
            log.info("Processing partition ${it.partition()}")
            // Assignation de la partition
            val topicPartition = TopicPartition(topic, it.partition())
            kafkaConsumer.assign(mutableListOf(topicPartition))
            results[it.partition()] = pollMessages(kafkaConsumer, topic, groupId)
            log.info("End of work for partition ${it.partition()}")
        }

        closeConsumer(kafkaConsumer)

        return results
    }

    /**
     * Renvoie les messages d'un topic, sur UNE seule partition, à partir d'un offset donné
     */
    @GetMapping("/messages/{topic}/{partition}")
    fun messagesForPartition(@PathVariable(value = "topic") topic: String,
                             @PathVariable(value = "partition") partition: Int,
                             @RequestParam(value = "groupId", defaultValue = "myGroup") groupId: String,
                             @RequestParam(value = "offset", defaultValue = "-2") offset: Long,
                             @RequestParam(value = "limit", defaultValue = "10") limit: Int): List<TopicMessage> {

        val kafkaConsumer = getKafkaConsumer(groupId)

        // Assignation de la partition qui nous intéresse
        val topicPartition = TopicPartition(topic, partition)
        kafkaConsumer.assign(mutableListOf(topicPartition))
        val oldOffsetsInformation = setOffset(kafkaConsumer, topicPartition, offset)
        log.info("Partition ${topicPartition.partition()} : Current offset is ${oldOffsetsInformation.position} " +
                "Committed offset is ->${oldOffsetsInformation.committed}")

        val polled = pollMessages(kafkaConsumer, topic, groupId)

        // Messages
        return polled.subList(0, min(polled.size, limit))
    }

    /**
     * Renvoie la liste de tous les messages d'un topic, toutes partitions confondues
     */
    @GetMapping("/offsets/{topic}")
    fun offsetForTopic(@PathVariable(value = "topic") topic: String,
                       @RequestParam(value = "groupId", defaultValue = "myGroup") groupId: String): TopicGroupOffsetInformation {

        val kafkaConsumer = getKafkaConsumer(groupId)
        val partitionOffsetInformation = mutableListOf<TopicGroupOffsetInformation.PartitionOffsetInformation>()
        val results = TopicGroupOffsetInformation(topic, groupId, partitionOffsetInformation)

        kafkaConsumer.partitionsFor(topic).forEach {
            log.info("Processing partition ${it.partition()}")
            // Assignation de la partition
            val topicPartition = TopicPartition(topic, it.partition())
            kafkaConsumer.assign(mutableListOf(topicPartition))
            partitionOffsetInformation.add(TopicGroupOffsetInformation.PartitionOffsetInformation(it.partition(), kafkaConsumer.committed(topicPartition)?.offset()))
            log.info("End of work for partition ${it.partition()}")
        }

        // Tri du tableau par partition
        partitionOffsetInformation.sortBy {
            it.partition
        }

        closeConsumer(kafkaConsumer)

        return results
    }

    /**
     * Commit l'offset d'un topic, pou UN groupe, sur UNE seule partition, à partir d'UN offset donné
     */
    @PostMapping("/offsets/{topic}/{partition}")
    fun commitForPartition(@PathVariable(value = "topic") topic: String,
                           @PathVariable(value = "partition") partition: Int,
                           @RequestParam(value = "groupId", defaultValue = "myGroup") groupId: String,
                           @RequestParam(value = "offset", defaultValue = "-2") offset: Long): CommitOffsetInformation {

        val kafkaConsumer = getKafkaConsumer(groupId)

        // Assignation de la partition qui nous intéresse
        val topicPartition = TopicPartition(topic, partition)
        kafkaConsumer.assign(mutableListOf(topicPartition))
        val oldOffsetsInformation = setOffset(kafkaConsumer, topicPartition, offset)
        log.info("Partition ${topicPartition.partition()} : Current offset is now ${oldOffsetsInformation.position}. " +
                "Committed offset is still ->${oldOffsetsInformation.committed}")
        kafkaConsumer.commitSync(mutableMapOf(Pair(topicPartition, OffsetAndMetadata(oldOffsetsInformation.position, ""))))
        val newCommittedOffset = kafkaConsumer.committed(topicPartition)?.offset()
        log.info("Partition ${topicPartition.partition()} : Current committed offset is now ->$newCommittedOffset")

        return CommitOffsetInformation(newCommittedOffset, topic, groupId, partition, oldOffsetsInformation.committed)
    }


    private fun pollMessages(kafkaConsumer: KafkaConsumer<String, String>, topic: String, groupId: String): MutableList<TopicMessage> {
        val partResult = mutableListOf<TopicMessage>()
        var encoreDuTravail = true
        while (encoreDuTravail) {
            val polled = kafkaConsumer.poll(Duration.ofMillis(200))
            polled.forEach {
                partResult += TopicMessage(topic, groupId, it.partition(), it.offset(), it.timestamp(), it.key(), it.value())
            }
            encoreDuTravail = !polled.isEmpty
        }
        return partResult
    }

    private fun getKafkaConsumer(groupId: String): KafkaConsumer<String, String> {
        // Configuration
        kafkaConsumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = groupId

        // Création du consumer avec la config à jour
        return KafkaConsumer(kafkaConsumerConfig)
    }

    private fun closeConsumer(kafkaConsumer: KafkaConsumer<String, String>) {
        log.info("Closing KafkaConsumer")
        kafkaConsumer.close(Duration.ofSeconds(10))
    }

    /**
     * Renvoie l'offset commité ainsi que l'offset courant (après modification éventuelle)
     */
    private fun setOffset(kafkaConsumer: KafkaConsumer<String, String>, topicPartition: TopicPartition, offset: Long): OffsetsInformation {
        val committed = kafkaConsumer.committed(topicPartition)

        when (offset) {
            -2L ->  {
                log.info("Leaving offset alone")
                // Si on ne souhaite pas modifier l'offset (= -2) et qu'on a jamais lu ce topic-groupe-partition
                // On choisi de dire qu'on commence au début et non à la fin (=mode latest arrangé)
                if (committed == null) {
                    log.info("Setting offset to beginning even if latest mode is active")
                    kafkaConsumer.seekToBeginning(mutableListOf(topicPartition))
                }
            }
            0L -> {
                log.info("Setting offset to begining")
                kafkaConsumer.seekToBeginning(mutableListOf(topicPartition))
            }
            -1L -> {
                log.info("Setting it to the end")
                kafkaConsumer.seekToEnd(mutableListOf(topicPartition))
            }
            else -> {
                log.info("Resetting offset to $offset")
                kafkaConsumer.seek(topicPartition, offset)
            }
        }
        return OffsetsInformation(kafkaConsumer.position(topicPartition), committed?.offset())
    }
}
