package fr.techos.kafkapi.handler

import fr.techos.kafkapi.config.KafkaConfig
import fr.techos.kafkapi.convert.Avro2Json
import fr.techos.kafkapi.helper.KafkaConsumerHelper
import fr.techos.kafkapi.model.TopicMessage
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.BodyInserters.fromObject
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.ok
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import java.nio.ByteBuffer
import java.time.Duration
import kotlin.math.min

inline fun <reified T: Any> typeRef(): ParameterizedTypeReference<T> = object: ParameterizedTypeReference<T>(){}

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

        val kafkaConsumer = KafkaConsumer<ByteArray?, ByteArray>(kafkaConsumerConfig)

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



    fun reactiveMessagesForTopic(request: ServerRequest): Mono<ServerResponse> {
        val topic: String = request.pathVariable("topic")
        val group: String = request.queryParam("group").orElse("myGroup")

        val kafkaConsumerConfig = kafkaConfig.getKafkaConsumerConfig()

        if (group.isEmpty()) {
            kafkaConsumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = "myGroup"
        } else {
            kafkaConsumerConfig[ConsumerConfig.GROUP_ID_CONFIG] = group
        }

        val receiverOptions = ReceiverOptions
                .create<String, String>(kafkaConsumerConfig)
                .subscription(mutableListOf(topic))

        val receiverFlux = KafkaReceiver
                .create(receiverOptions)
                .receive()
                .map { record -> record.value() }
                .doOnNext { message -> logger.info { "Received message: $message" } }

        return ok()
                .contentType(MediaType.TEXT_EVENT_STREAM)
                .body(receiverFlux, String::class.java)

//        return ok()
//                .body(receiverFlux, typeRef<ReceiverRecord<String, String>>())
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

        val kafkaConsumer = KafkaConsumer<ByteArray?, ByteArray>(kafkaConsumerConfig)

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

    private fun pollMessages(kafkaConsumer: KafkaConsumer<ByteArray?, ByteArray>, topic: String, group: String): MutableList<TopicMessage> {
        val partResult = mutableListOf<TopicMessage>()
        var workToDo = true
        val a2j = Avro2Json(CachedSchemaRegistryClient("http://schema.suez.int.net-courrier.extra.laposte.fr:8081", 30))
        while (workToDo) {
            val polled = kafkaConsumer.poll(Duration.ofMillis(2000))
            polled.forEach {
                val key = it.key()?.let { it1 -> a2j.convert(it1) } ?: ""
                val message = a2j.convert(it.value())
                partResult += TopicMessage(topic,
                        group,
                        it.partition(),
                        it.offset(),
                        it.timestamp(),
                        key,
                        message,
                        retrieveAvroSchemaId(it.value()))
            }
            workToDo = !polled.isEmpty
        }
        return partResult
    }


    protected fun retrieveAvroSchemaId(payload: ByteArray): Int {

            val MAGIC_BYTE: Byte = 0x0
            val buffer = ByteBuffer.wrap(payload)
            if (buffer.get() != MAGIC_BYTE) {
                return -1
            }
            return buffer.int

    }
}