package fr.techos.kafkapi.config

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import java.util.*

@Configuration
@EnableKafka
@EnableConfigurationProperties(KafkaProperties::class)
class KafkaConfig {
    @Autowired
    lateinit var kafkaProperties: KafkaProperties

    @Bean
    fun kafkaConsumerConfig(): Properties {
        val properties = Properties()
        properties.putAll(kafkaProperties.buildConsumerProperties())
        return properties
    }

}