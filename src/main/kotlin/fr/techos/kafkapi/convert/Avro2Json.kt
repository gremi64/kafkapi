package fr.techos.kafkapi.convert

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.common.errors.SerializationException
import java.nio.charset.StandardCharsets

class Avro2Json(private val schemaRegistry: SchemaRegistryClient) {


    /**
     * Convert byteArray to Json (as a String)
     * @param value ByteArray
     * @return String
     */
    fun convert(value: ByteArray): String {
        try {
            KafkaAvroDeserializer(this.schemaRegistry).use { deserializer ->
                val deserialized = deserializer.deserialize("", value)
                return objectToJson(deserialized)
            }
        } catch (e: SerializationException) {

            if (e.message.isNullOrBlank()) {
                throw e
            }

            val isAvro = !(e.message!!.contains("-1"))

            // En cas d'erreur de déserialisation
            // Si le message contient -1 et que la cause est une erreur de sérialisation avec en message "Unknown magic byte!"
            //      C'est good ! Il ne s'agit pas d'avro !
            //      -> byteArrayToString(value)
            if (!isAvro
                    && e.cause != null
                    && e.cause is SerializationException
                    && "Unknown magic byte!" == e.cause!!.message) {
                return byteArrayToString(value)
            }

            // Si aucun des cas précédent on remonte l'erreur
            throw e
        }
    }

    /**
     * Return JSON string of given object
     * @param obj object we want to be converted to string
     * @return JSON string of object
     */
    private fun objectToJson(obj: Any): String {
        return obj.toString()
    }

    /**
     * Extract JSON String from byte array
     * @param value byte array
     * @return deserialized string
     */
    private fun byteArrayToString(value: ByteArray): String {
        return String(value, StandardCharsets.UTF_8)
    }

}
