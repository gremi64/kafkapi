package fr.techos.kafkapi.handler

import fr.techos.kafkapi.config.KafkaConfig
import fr.techos.kafkapi.helper.KafkaConsumerHelper
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters.fromObject
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.ok
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono

data class CommitBody(val offset: Long, val group: String, val brokers: String?, val security: String?)

@Component
class OffsetsHandler(val kafkaConfig: KafkaConfig) {

    /**
     * Commit l'offset d'un topic, pou UN groupe, sur UNE seule partition, à partir d'UN offset donné
     */
    fun commitForPartition(request: ServerRequest): Mono<ServerResponse> {
        val topic: String = request.pathVariable("topic")
        val partition = request.pathVariable("partition").toInt()

        return request.bodyToMono(CommitBody::class.java)
            .map { body ->
                val kafkaConsumerConfig = this.kafkaConfig.getConfigWithGivenParams(body.group, body.brokers, body.security)
                KafkaConsumerHelper.commitResult(kafkaConsumerConfig, topic, partition, body.group, body.offset)
            }
            .flatMap { commitResult -> ok().body(fromObject(commitResult)) }
    }

    /**
     * Renvoie la liste de tous les messages d'un topic, toutes partitions confondues
     */
    fun offsetForTopic(request: ServerRequest): Mono<ServerResponse> {
        val topic: String = request.pathVariable("topic")
        val group: String = request.queryParam("group").orElse("myGroup")
        val brokers: String? = request.queryParam("brokers").orElse(null)
        val security: String? = request.queryParam("security").orElse(null)

        val kafkaConsumerConfig = this.kafkaConfig.getConfigWithGivenParams(group, brokers, security)

        val results = KafkaConsumerHelper.getTopicOffsetsForGroup(topic, group, kafkaConsumerConfig)

        return ok().body(fromObject(results)).toMono()
    }


}