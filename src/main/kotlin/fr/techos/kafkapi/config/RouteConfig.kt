package fr.techos.kafkapi.config

import fr.techos.kafkapi.handler.ConfigHandler
import fr.techos.kafkapi.handler.MessagesHandler
import fr.techos.kafkapi.handler.OffsetsHandler
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.router

@Configuration
class RouteConfig {

    @Bean
    fun offsetsRouter(handler: OffsetsHandler) = router {
        ("/offsets" and accept(MediaType.APPLICATION_JSON)).nest {
            GET("/{topic}", handler::offsetForTopic)
            POST("/{topic}/{partition}", handler::commitForPartition)
        }
    }

    @Bean
    fun messagesRouter(handler: MessagesHandler) = router {
        ("/messages" and accept(MediaType.APPLICATION_JSON)).nest {
            GET("/{topic}", handler::messagesForTopic)
            GET("/reactive/{topic}", handler::reactiveMessagesForTopic)
            GET("/{topic}/{partition}", handler::messagesForPartition)
        }
    }

    @Bean
    fun configRouter(handler: ConfigHandler) = router {
        ("/config" and accept(MediaType.APPLICATION_JSON)).nest {
            GET("/consumers", handler::getConsumerConfigs)
        }
    }

}
