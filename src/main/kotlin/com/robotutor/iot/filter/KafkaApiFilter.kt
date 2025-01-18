package com.robotutor.iot.filter

import com.robotutor.iot.services.KafkaPublisher
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono

@Component
@Order(Ordered.LOWEST_PRECEDENCE)
class KafkaApiFilter(private val kafkaPublisher: KafkaPublisher) : WebFilter {

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        return chain.filter(exchange)
            .contextWrite {
                it.put(KafkaPublisher::class.java, kafkaPublisher)
            }
    }
}
