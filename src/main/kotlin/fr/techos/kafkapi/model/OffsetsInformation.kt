package fr.techos.kafkapi.model

data class OffsetsInformation (val position: Long,
                               val committed: Long?)