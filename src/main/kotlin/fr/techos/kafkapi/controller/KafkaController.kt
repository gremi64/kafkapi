package fr.techos.kafkapi.controller


import fr.techos.kafkapi.model.TopicMessage
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.GetMapping
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

    @GetMapping("/message")
    fun message(@RequestParam(value = "topic", defaultValue = "he_dev_notifFeasibilityEvent_out_v1") topic: String,
                @RequestParam(value = "groupId", defaultValue = "he_dev_ud_labelbox") groupId: String,
                @RequestParam(value = "offset", defaultValue = "-2") offset: Long,
                @RequestParam(value = "partition", defaultValue = "0") partition: Int): MutableList<TopicMessage> {

        //val topicPartition = TopicPartition(topic, partition)

        kafkaConsumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        val kafkaConsumer = KafkaConsumer<String, String>(kafkaConsumerConfig)

        kafkaConsumer.subscribe(mutableListOf(topic), object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                log.info("${mutableListOf(partitions)} topic-partitions are revoked from this consumer")
            }

            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                log.info("${mutableListOf(partitions)} topic-partitions are assigned to this consumer")
                partitions
                    .filter { it.partition() == partition }
                    .forEach {
                        log.info("Partition ${it.partition()} : Current offset is ${kafkaConsumer.position(it)} committed offset is ->${kafkaConsumer.committed(it)}")
                        when (offset) {
                            -2L -> log.info("Leaving it alone")
                            0L -> {
                                log.info("Setting offset to begining")
                                kafkaConsumer.seekToBeginning(mutableListOf(it))
                            }
                            -1L -> {
                                log.info("Setting it to the end")
                                kafkaConsumer.seekToEnd(mutableListOf(it))
                            }
                            else -> {
                                log.info("Resetting offset to $offset")
                                kafkaConsumer.seek(it, offset)
                            }
                        }
                    }
            }
        })

        val results = mutableListOf<TopicMessage>()
        kafkaConsumer
                .poll(Duration.ofSeconds(10))
                .forEach {
                    results += TopicMessage(topic, groupId, it.partition(), it.offset(), it.timestamp(), it.key(), it.value())
                }

        kafkaConsumer.close(Duration.ofSeconds(10))

        return results
    }

}