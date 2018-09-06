package fr.techos.kafkapi.controller


import fr.techos.kafkapi.model.TopicMessage
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Duration
import java.util.*


@RestController
class KafkaController {

    @Autowired
    lateinit var kafkaConsumerConfig: Properties

    @GetMapping("/message")
    fun message(@RequestParam(value = "topicName", defaultValue = "myTopic") topicName: String,
                @RequestParam(value = "groupId", defaultValue = "myGroup") groupId: String,
                @RequestParam(value = "offset", defaultValue = "0") offset: Int): MutableList<TopicMessage> {

        kafkaConsumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        val kafkaConsumer = KafkaConsumer<String,String>(kafkaConsumerConfig)

        kafkaConsumer.subscribe(mutableListOf(topicName))

        val results = mutableListOf<TopicMessage>()

        kafkaConsumer
            .poll(Duration.ofMillis(1000))
            .forEach {
                results += TopicMessage(topicName, groupId, it.partition(), it.offset(), it.timestamp(), it.key(), it.value())
            }

        kafkaConsumer.close(Duration.ofSeconds(10))

        return results
    }

}