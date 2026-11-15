package io.github.nomisrev.kafka.publisher

import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisrev.kafka.KafkaSpec
import io.kotest.core.annotation.Isolate
import kotlinx.coroutines.CoroutineStart.UNDISPATCHED
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.kafka.clients.producer.ProducerConfig

/** Pauses the shared broker. */
@Isolate
class IdempotentPublisherSpec : KafkaSpec({
  test("idempotent publisher") {
    withTopic {
      val records = produce(10)
      launch(start = UNDISPATCHED) {
        KafkaPublisher(publisherSettings {
//          put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
          put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
        }).use {
          it.publishScope {
            records.forEach { r ->
              offer(r)
              delay(100)
            }
          }
        }
      }

      kafka.pause()
      delay(2000)
      kafka.unpause()
      topic.assertHasRecords(records)
    }
  }
})
