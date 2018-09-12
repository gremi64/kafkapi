package fr.techos.kafkapi.config

import mu.KotlinLogging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
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
@EnableConfigurationProperties(KafkaProperties::class, KafkaSecurityProperties::class, KafkaEnvProperties::class)
class KafkaConfig {
    private val logger = KotlinLogging.logger {}

    @Autowired
    lateinit var kafkaProperties: KafkaProperties

    @Autowired
    lateinit var kafkaSecurityProperties: KafkaSecurityProperties


    @Autowired
    lateinit var kafkaEnvProperties: KafkaEnvProperties

    @Bean
    fun kafkaConsumerConfig(): Properties {
        val properties = Properties()
        properties.putAll(kafkaProperties.buildConsumerProperties())
        // Environment
        properties.putAll(this.kafkaEnvConfig())
        return properties
    }

    private fun kafkaEnvConfig(): Properties {
        val properties = Properties()
        logger.info { "Selected environment : ${this.kafkaEnvProperties.selected}" }
        val prop = this.kafkaEnvProperties.available[this.kafkaEnvProperties.selected]
        prop?.let {
            with(properties) {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, it.bootstrapServers)
                putAll(it.properties)
                if ("SSL" == it.properties[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG]) {
                    putAll(kafkaSecurityConfig())
                }
            }
        }
        logger.debug { "Environment properties officially loaded : $properties" }
        return properties
    }

    private fun kafkaSecurityConfig(): Properties {
        val properties = Properties()
        logger.info { "Selected security user : ${this.kafkaSecurityProperties.domain}" }
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
        // Attention a l'affichage du mot de passe...
        logger.debug { "Security properties officially loaded : $properties" }
        return properties
    }
}