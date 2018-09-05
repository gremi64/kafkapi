package fr.techos.kafkapi

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkapiApplication

fun main(args: Array<String>) {
    runApplication<KafkapiApplication>(*args)
}
