package fr.techos.kafkapi.config

import mu.KotlinLogging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import java.util.*

@Configuration
@EnableKafka
@EnableConfigurationProperties(KafkaProperties::class, KafkaSecurityProperties::class)
class KafkaConfig {
    private val logger = KotlinLogging.logger {}

    @Autowired
    lateinit var kafkaProperties: KafkaProperties

    @Autowired
    lateinit var kafkaSecurityProperties: KafkaSecurityProperties

    @Bean
    fun kafkaConsumerConfig(): Properties {
        val properties = Properties()
        properties.putAll(kafkaProperties.buildConsumerProperties())
        if ("SSL" == kafkaProperties.properties[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG]) {
            properties.putAll(this.kafkaSecurityConfig())
        }
        return properties
    }

    private fun kafkaSecurityConfig(): Properties {
        val properties = Properties()
        val prop = this.kafkaSecurityProperties.user[this.kafkaSecurityProperties.domain]
        prop?.let {
            with(properties) {
                put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, it.keyPassword)
                put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, it.keystoreLocation)
                put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, it.keystorePassword)
                put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, it.truststoreLocation)
                put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, it.truststorePassword)
            }
        }
        logger.info { "Security properties officially loaded : $properties" }
        return properties
    }

}