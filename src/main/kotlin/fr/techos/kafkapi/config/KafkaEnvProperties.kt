package fr.techos.kafkapi.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration
import java.util.*

@Configuration
@ConfigurationProperties(prefix = "kafkapi.kafka.env")
class KafkaEnvProperties {
    lateinit var selected: String

    val available: MutableMap<String, KafkaAvailableEnvProperties> = mutableMapOf()

    class KafkaAvailableEnvProperties {
        lateinit var bootstrapServers: String
        lateinit var properties: Properties
    }

}