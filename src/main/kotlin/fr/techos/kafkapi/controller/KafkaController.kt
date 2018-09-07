package fr.techos.kafkapi.controller


import fr.techos.kafkapi.model.TopicMessage
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Duration
import java.util.*
import java.util.logging.Logger


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
                         @RequestParam(value = "offset", defaultValue = "-2") offset: Long,
                         @RequestParam(value = "limit", defaultValue = "1000") limit: Int): MutableMap<Int, List<TopicMessage>> {

        val kafkaConsumer = getKafkaConsumer(groupId, limit)
        val results = mutableMapOf<Int, List<TopicMessage>>()

        kafkaConsumer.partitionsFor(topic).forEach {
            log.info("Processing partition ${it.partition()}")
            // Assignation de la partition
            val topicPartition = TopicPartition(topic, it.partition())
            kafkaConsumer.assign(mutableListOf(topicPartition))
            val partResult = mutableListOf<TopicMessage>()
            var encoreDuTravail = true
            while (encoreDuTravail) {
                val polled = kafkaConsumer.poll(Duration.ofMillis(200))
                polled.forEach {
                    partResult += TopicMessage(topic, groupId, it.partition(), it.offset(), it.timestamp(), it.key(), it.value())
                }
                encoreDuTravail = !polled.isEmpty
            }
            results[it.partition()] = partResult
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
                             @RequestParam(value = "limit", defaultValue = "10") limit: Int): MutableList<TopicMessage> {

        val kafkaConsumer = getKafkaConsumer(groupId, limit)

        // Assignation de la partition qui nous intéresse
        val topicPartition = TopicPartition(topic, partition)
        kafkaConsumer.assign(mutableListOf(topicPartition))
        setOffset(kafkaConsumer, topicPartition, offset)
        log.info("Partition ${topicPartition.partition()} : Current offset is ${kafkaConsumer.position(topicPartition)} committed offset is ->${kafkaConsumer.committed(topicPartition)}")

        // Messages
        val results = mutableListOf<TopicMessage>()
        kafkaConsumer
                .poll(Duration.ofSeconds(10))
                .forEach {
                    results += TopicMessage(topic, groupId, it.partition(), it.offset(), it.timestamp(), it.key(), it.value())
                }
        closeConsumer(kafkaConsumer)

        return results
    }

    private fun getKafkaConsumer(groupId: String, limit: Int = 1000): KafkaConsumer<String, String> {
        // Configuration
        kafkaConsumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        kafkaConsumerConfig[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = limit

        // Création du consumer avec la config à jour
        return KafkaConsumer(kafkaConsumerConfig)
    }

    private fun closeConsumer(kafkaConsumer: KafkaConsumer<String, String>) {
        log.info("Closing KafkaConsumer")
        kafkaConsumer.close(Duration.ofSeconds(10))
    }

    private fun setOffset(kafkaConsumer: KafkaConsumer<String, String>, topicPartition: TopicPartition, offset: Long) {
        when (offset) {
            -2L -> log.info("Leaving it alone")
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
    }
}
