package com.kotlin.io.example

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.pulsar.io.core.Sink
import org.apache.pulsar.functions.api.Record
import org.apache.pulsar.io.common.IOConfigUtils
import org.apache.pulsar.io.core.SinkContext
import org.apache.pulsar.io.core.annotations.FieldDoc

data class SinkConfig @JsonCreator constructor(
    @JsonProperty("address")
    @FieldDoc(
        required = true,
        defaultValue = "",
        help = ""
    )
    val address: List<String>,
    @JsonProperty("nested")
    @FieldDoc(
        required = true,
        defaultValue = "",
        help = ""
    )
    val nested: Map<String, Int>
)


class ExampleSink: Sink<Map<String, Any>> {

    override fun open(config: MutableMap<String, Any>, sinkContext: SinkContext?) {
        
        // Not recommended
        val mapper = ObjectMapper()
        mapper.readValue(mapper.writeValueAsString(config), SinkConfig::class.java)
        
        // recommend user IOUtils to read config map
        val loadWithSecrets = IOConfigUtils.loadWithSecrets(config, SinkConfig::class.java, sinkContext)

        println("HELLO") // just here as an example
    }

    override fun write(record: Record<Map<String, Any>>) {
        // write something        
    }

    override fun close() {
        // close something        
    }
}