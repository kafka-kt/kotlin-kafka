package io.github.nomisrev.kafka

import io.kotest.extensions.testcontainers.TestContainerProjectExtension
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName
import kotlin.time.Duration.Companion.seconds

class Kafka : ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.12")) {

  fun pause() {
    dockerClient.pauseContainerCmd(containerId).exec()
  }

  fun unpause() {
    dockerClient.unpauseContainerCmd(containerId).exec()
  }
}

/** Broker shared by every spec. A spec that changes the broker itself (e.g. pausing it) must be `@Isolate`. */
object KafkaContainer {
  private val transactionTimeoutInterval = 1.seconds

  val kafka: Kafka =
    Kafka().apply {
      withEnv(
        "KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS",
        transactionTimeoutInterval.inWholeMilliseconds.toString()
      )
      withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
      withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true")
    }

  /** One instance, so its lock serialises concurrent specs' first `start()`. */
  val extension: TestContainerProjectExtension<Kafka> = TestContainerProjectExtension(kafka)
}
