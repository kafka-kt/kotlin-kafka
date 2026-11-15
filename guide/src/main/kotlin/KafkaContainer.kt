import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * A singleton `Kafka` Test Container, started on first use. No need to `close` or `stop` it: Testcontainers removes
 * it when the JVM exits.
 *
 * ```kotlin
 * class MySpec : StringSpec({
 *   val kafka = Kafka.container
 *   ...
 * })
 * ```
 *
 * @see https://java.testcontainers.org/test_framework_integration/manual_lifecycle_control/
 */
class Kafka private constructor(imageName: DockerImageName) : ConfluentKafkaContainer(imageName) {

  companion object {
    private val image: DockerImageName = DockerImageName.parse("confluentinc/cp-kafka:7.6.12")

    val container: ConfluentKafkaContainer by lazy {
      Kafka(image).also { it.start() }
    }
  }
}
