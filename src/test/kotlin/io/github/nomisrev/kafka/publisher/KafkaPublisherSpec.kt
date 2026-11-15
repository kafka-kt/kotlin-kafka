package io.github.nomisrev.kafka.publisher

import io.github.nomisRev.kafka.publisher.Acks
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisrev.kafka.Boom
import io.github.nomisrev.kafka.KafkaSpec
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart.UNDISPATCHED
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.ProducerFencedException

class KafkaPublisherSpec : KafkaSpec({
  test("All offered messages are received") {
    withTopic {
      val records = produce(10_000)
      publishScope {
        offer(records)
      }

      topic.assertHasRecords(records)
    }
  }

  test("Can receive all messages that were published on the right partitions") {
    withTopic {
      val records = produce(3)
      publishScope {
        publish(records)
      }

      topic.assertHasRecords(records)
    }
  }

  test("A failure in a produce block, rethrows the error") {
    withTopic {
      val record = createProducerRecord(0)

      val exception = shouldThrow<RuntimeException> {
        publishScope<Unit> {
          offer(record)
          throw Boom
        }
      }

      exception shouldBe Boom
      topic.assertHasRecord(record)
    }
  }

  test("A failure in a produce block with a concurrent launch cancels the launch, rethrows the error") {
    withTopic {
      val cancelSignal = CompletableDeferred<CancellationException>()
      val exception = shouldThrow<RuntimeException> {
        publishScope<Unit> {
          launch(start = UNDISPATCHED) {
            try {
              awaitCancellation()
            } catch (e: CancellationException) {
              cancelSignal.complete(e)
              throw e
            }
          }
          throw Boom
        }
      }

      exception shouldBe Boom
      cancelSignal.await()
    }
  }

  test("A failed offer is rethrown at the end") {
    withTopic {
      val record = createProducerRecord(0)

      val exception = shouldThrow<RuntimeException> {
        KafkaPublisher(publisherSettings(), stubProducer(failOnNumber = 0)).use {
          it.publishScope {
            offer(record)
          }
        }
      }

      exception shouldBe Boom
    }
  }

  test("An async failure is rethrow at the end") {
    withTopic {
      val records = produce(3)

      val exception = shouldThrow<RuntimeException> {
        publishScope {
          publish(records)
          launch { throw Boom }
        }
      }

      exception shouldBe Boom
      topic.assertHasRecords(records)
    }
  }

  test("A failure of a sendAwait can be caught in the block") {
    withTopic {
      val record0 = createProducerRecord(0)
      val record1 = createProducerRecord(1)

      KafkaPublisher(publisherSettings(), stubProducer(failOnNumber = 0)).use {
        it.publishScope {
          publishCatching(record0)
          offer(record1)
        }
      }

      topic.assertHasRecord(record1)
    }
  }

  test("concurrent publishing") {
    withTopic {
      val count = 4
      val records =
        (0..<count).map { base ->
          (base..base + count).map(::createProducerRecord)
        }

      publishScope {
        listOf(
          async { offer(records[0]) },
          async { offer(records[1]) },
          async { publish(records[2]) },
          async { publish(records[3]) }
        ).awaitAll()
      }

      topic.assertHasRecords(records)
    }
  }

  test("transaction an receive all messages that were published on the right partitions") {
    withTopic {
      val count = 3
      val records = (0..count).map(::createProducerRecord)
      val settings = publisherSettings(Acks.All) {
        put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction an receive all messages")
        put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
      }

      KafkaPublisher(settings).use {
        it.publishScope {
          transaction {
            offer(records)
          }
        }
      }

      topic.assertHasRecords(records)
    }
  }

  test("A failure in a transaction aborts the transaction") {
    withTopic {
      val records = produce(3)
      val settings = publisherSettings(Acks.All) {
        put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "A failure in a transaction aborts")
        put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
      }
      val exception = shouldThrow<RuntimeException> {
        KafkaPublisher(settings).use {
          it.publishScope<Unit> {
            transaction {
              publish(records)
              throw Boom
            }
          }
        }
      }

      exception shouldBe Boom
      topic.shouldBeEmpty()
    }
  }

  test("An async failure in a transaction aborts the transaction") {
    withTopic {
      val records = produce(3)
      val settings = publisherSettings(Acks.All) {
        put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "An async failure in a transaction aborts")
        put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
      }
      val exception = shouldThrow<RuntimeException> {
        KafkaPublisher(settings).use {
          it.publishScope {
            transaction {
              offer(records)
              launch { throw Boom }
            }
          }
        }
      }

      exception shouldBe Boom
      topic.shouldBeEmpty()
    }
  }

  test("transaction - concurrent publishing") {
    withTopic {
      val count = 4
      val records =
        (0..<count).map { base ->
          (base..base + count).map(::createProducerRecord)
        }

      val settings = publisherSettings(Acks.All) {
        put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction - concurrent publishing")
        put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
      }

      KafkaPublisher(settings).use {
        it.publishScope {
          transaction {
            listOf(
              async { offer(records[0]) },
              async { offer(records[1]) },
              async { publish(records[2]) },
              async { publish(records[3]) }
            ).awaitAll()
          }
        }
      }

      topic.assertHasRecords(records)
    }
  }

  test("Only one KafkaProducer can have transactional_id at once, ProducerFencedException is fatal") {
    withTopic {
      val settings = publisherSettings(Acks.All) {
        put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "Only one KafkaProducer can have transactional.id")
        put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
      }
      val records1 = produce(0..4)
      val publisher1 = KafkaPublisher(settings)
      publisher1.publishScope {
        transaction {
          publish(records1)
        }
      }

      val records2 = produce(5..9)
      val publisher2 = KafkaPublisher(settings)
      publisher2.publishScope {
        transaction {
          publish(records2)
        }
      }

      // publisher1 was previous transactional.id, will result in fatal ProducerFencedException
      val records3 = produce(10..14)
      shouldThrow<ProducerFencedException> {
        publisher1.publishScope {
          transaction {
            publishCatching(records3)
          }
        }
      }

      // Due to ProducerFencedException, only records1 and records2 are received
      topic.assertHasRecords(records1 + records2)
    }
  }
})
