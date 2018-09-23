package fr.techos.kafkapi.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "kafkapi.kafka.security")
class KafkaSecurityProperties {
    lateinit var domain: String

    val user: MutableMap<String, KafkaSslProperties> = mutableMapOf()

    class KafkaSslProperties {
        lateinit var description: String
        lateinit var securityProtocol: String
        lateinit var keyPassword: String
        lateinit var keystoreLocation: String
        lateinit var keystorePassword: String
        lateinit var truststoreLocation: String
        lateinit var truststorePassword: String
    }
}