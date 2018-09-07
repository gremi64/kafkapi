package fr.techos.kafkapi.controller


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
                         @RequestParam(value = "groupId", defaultValue = "myGroup") groupId: String,
                         @RequestParam(value = "offset", defaultValue = "-2") offset: Long): MutableMap<Int, List<TopicMessage>> {

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
        val position = setOffset(kafkaConsumer, topicPartition, offset)
        log.info("Partition ${topicPartition.partition()} : Current offset is $position committed offset is ->${kafkaConsumer.committed(topicPartition).offset()}")

        val polled = pollMessages(kafkaConsumer, topic, groupId)

        // Messages
        return polled.subList(0, min(polled.size, limit))
    }

    /**
     * Commit l'offset d'un topic, pou UN groupe, sur UNE seule partition, à partir d'UN offset donné
     */
    @PostMapping("/messages/{topic}/{partition}")
    fun commitForPartition(@PathVariable(value = "topic") topic: String,
                             @PathVariable(value = "partition") partition: Int,
                             @RequestParam(value = "groupId", defaultValue = "myGroup") groupId: String,
                             @RequestParam(value = "offset", defaultValue = "-2") offset: Long): List<TopicMessage> {

        // Limite non modifiable de UN message
        // Pour ne pas perdre l'utilisateur qui pourrait penser que les messages affichés sont commités
        val limit = 1
        val kafkaConsumer = getKafkaConsumer(groupId)

        // Assignation de la partition qui nous intéresse
        val topicPartition = TopicPartition(topic, partition)
        kafkaConsumer.assign(mutableListOf(topicPartition))
        val position = setOffset(kafkaConsumer, topicPartition, offset)
        log.info("Partition ${topicPartition.partition()} : Current offset is $position committed offset is ->${kafkaConsumer.committed(topicPartition)}")
        val polled = pollMessages(kafkaConsumer, topic, groupId)
        kafkaConsumer.commitSync(mutableMapOf(Pair(topicPartition, OffsetAndMetadata(position, ""))))
        log.info("Partition ${topicPartition.partition()} : Current committed offset is ->${kafkaConsumer.committed(topicPartition).offset()}")

        // Messages
        return polled.subList(0, min(polled.size, limit))
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

    private fun setOffset(kafkaConsumer: KafkaConsumer<String, String>, topicPartition: TopicPartition, offset: Long): Long {
        when (offset) {
            -2L -> log.info("Leaving offset alone")
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
        return kafkaConsumer.position(topicPartition)
    }
}
