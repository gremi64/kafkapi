package fr.techos.kafkapi.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SslConfigs
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration
import java.util.*

@Configuration
@ConfigurationProperties(prefix = "kafkapi.kafka.security")
class KafkaSecurityProperties {
    lateinit var domain: String

    val user: MutableMap<String, KafkaSslProperties> = mutableMapOf()

    class KafkaSslProperties {
        lateinit var keyPassword: String
        lateinit var keystoreLocation: String
        lateinit var keystorePassword: String
        lateinit var truststoreLocation: String
        lateinit var truststorePassword: String
    }
}