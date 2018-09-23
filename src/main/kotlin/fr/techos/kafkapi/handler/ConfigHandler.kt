package fr.techos.kafkapi.handler

import fr.techos.kafkapi.config.KafkaEnvProperties
import fr.techos.kafkapi.config.KafkaSecurityProperties
import fr.techos.kafkapi.model.KafkaConsumerConfigs
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters.fromObject
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.ok
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import java.util.*

@Component
class ConfigHandler(
        val kafkaSecurityProperties: KafkaSecurityProperties,
        val kafkaEnvProperties: KafkaEnvProperties) {

    private val logger = KotlinLogging.logger {}

    fun getConsumerConfigs(request: ServerRequest): Mono<ServerResponse> {
        val securityConfigsList = mutableListOf<Pair<String, String>>()
        this.kafkaSecurityProperties.user.forEach { userKey, _ -> securityConfigsList.add(Pair(UUID.randomUUID().toString(), userKey)) }

        val brokersList = mutableListOf<Pair<String, String>>()
        this.kafkaEnvProperties.available.forEach { brokerAlias, properties -> brokersList.add(Pair(brokerAlias, properties.bootstrapServers)) }

        logger.info { "Sending config > ${KafkaConsumerConfigs(brokersList, securityConfigsList)}" }

        return ok().body(fromObject(KafkaConsumerConfigs(brokersList, securityConfigsList))).toMono()
    }
}