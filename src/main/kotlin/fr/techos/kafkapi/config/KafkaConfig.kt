package fr.techos.kafkapi.config

import mu.KotlinLogging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SslConfigs
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import java.util.*

@Configuration
@EnableKafka
@EnableConfigurationProperties(KafkaProperties::class, KafkaSecurityProperties::class, KafkaEnvProperties::class)
class KafkaConfig(val kafkaProperties: KafkaProperties,
                  val kafkaSecurityProperties: KafkaSecurityProperties,
                  val kafkaEnvProperties: KafkaEnvProperties) {

    private val logger = KotlinLogging.logger {}

    fun getKafkaConsumerConfig(): Properties {
        val properties = Properties()
        properties.putAll(kafkaProperties.buildConsumerProperties())
        properties.putAll(this.kafkaEnvConfig())
        return properties
    }

    fun getBootstrapServersForKey(key: String): Properties {
        val props = Properties()

        this.kafkaEnvProperties.available[key]?.bootstrapServers.let {
            props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = it
        }

        return props
    }

    fun getSecurityPropsForKey(key: String): Properties {
        val props = Properties()

        val prop = this.kafkaSecurityProperties.user[key]
        prop?.let {
            props[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = it.securityProtocol
            props[SslConfigs.SSL_PROTOCOL_CONFIG] = it.securityProtocol
            if (it.securityProtocol == "SSL") {
                props[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = it.keyPassword
                props[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] = it.keystoreLocation
                props[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] = it.keystorePassword
                props[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] = it.truststoreLocation
                props[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] = it.truststorePassword
            }
        }

        return props
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