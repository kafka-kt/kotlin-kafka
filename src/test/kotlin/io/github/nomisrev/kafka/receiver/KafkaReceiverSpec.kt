package io.github.nomisrev.kafka.receiver

import io.github.nomisRev.kafka.receiver.CommitStrategy
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisrev.kafka.Boom
import io.github.nomisrev.kafka.KafkaSpec
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.github.nomisrev.kafka.mapIndexed
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord

data class KeyValue(val key: String, val value: String)

@ExperimentalCoroutinesApi
class KafkaReceiverSpec : KafkaSpec({
  val count = 1000
  val lastIndex = count - 1
  fun produced(
    startIndex: Int = 0,
    lastIndex: Int = count,
  ): List<KeyValue> =
    (startIndex until lastIndex).map { n -> KeyValue("key-$n", "value->$n") }

  val produced = produced()

  test("All produced records are received") {
    withTopic {
      publishToKafka(topic, produced)
      KafkaReceiver()
        .receive(topic.name())
        .map { record ->
          yield()
          KeyValue(record.key(), record.value())
            .also { record.offset.acknowledge() }
        }.take(count)
        .toSet() shouldBe produced.toSet()
    }
  }

  test("All records produced while consuming are received") {
    withTopic {
      launch { publishToKafka(topic, produced) }
      KafkaReceiver()
        .receive(topic.name())
        .map { record ->
          KeyValue(record.key(), record.value())
            .also { record.offset.acknowledge() }
        }.take(count)
        .toSet() shouldBe produced.toSet()
    }
  }

  test("Continuous receiving does not overflow stack") {
    withTopic {
      val largeCount = 20_000 // when it _was_ overflowing stack, it only took ~4095 messages.
      publishScope {
        repeat(largeCount) {
          offer(ProducerRecord(topic.name(), "key-$it", "value-$it"))
        }
      }
      val settings = receiverSetting().copy(
        maxDeferredCommits = largeCount + 1,
        commitStrategy = CommitStrategy.BySize(largeCount + 1),
        properties = mapOf(ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "1").toProperties()
      )
      KafkaReceiver(settings)
        .receive(topic.name())
        .buffer(largeCount)
        .take(largeCount)
        .onEach { it.offset.acknowledge() }
        .count() shouldBe largeCount
    }
  }

  test("Can consume after backpressure") {
    withTopic {
      publishToKafka(topic, produced)
      KafkaReceiver()
        .receive(topic.name())
        .map { record ->
          yield()
          KeyValue(record.key(), record.value())
            .also { record.offset.acknowledge() }
        }.take(count)
        .toSet() shouldBe produced.toSet()
    }
  }

  test("Concurrent commits while receiving") {
    withTopic {
      publishToKafka(topic, produced)
      val receiver = KafkaReceiver(
        receiverSetting().copy(
          commitStrategy = CommitStrategy.BySize(count / 2)
        )
      )
      receiver.receive(topic.name())
        .take(count)
        .collect { it.offset.acknowledge() }

      receiver.committedCount(topic.name()) shouldBe count.toLong()
    }
  }

  test("All produced records with headers are received") {
    withTopic(partitions = 1) {
      val producerRecords = produced.map { (key, value) ->
        ProducerRecord(topic.name(), key, value).apply {
          headers().add("header1", byteArrayOf(0.toByte()))
          headers().add("header2", value.toByteArray())
        }
      }

      publishToKafka(producerRecords)

      val _ = KafkaReceiver()
        .receive(topic.name())
        .take(count)
        .onEach { it.offset.acknowledge() }
        .toList()
        .zip(producerRecords) { actual, expected ->
          actual.key() shouldBe expected.key()
          actual.value() shouldBe expected.value()
          actual.topic() shouldBe topic.name()
          actual.headers().toArray().size shouldBe 2
          actual.headers() shouldBe expected.headers()
        }
    }
  }

  test("Should receive all records when subscribing several consumers") {
    withTopic {
      publishToKafka(topic, produced)
      val consumer =
        KafkaReceiver()
          .receive(topic.name())
          .map {
            yield()
            KeyValue(it.key(), it.value())
              .also { _ -> it.offset.acknowledge() }
          }

      flowOf(consumer, consumer)
        .flattenMerge()
        .take(count)
        .toSet() shouldBe produced.toSet()
    }
  }

  test("All acknowledged messages are committed on flow completion") {
    withTopic {
      publishToKafka(topic, produced)
      val receiver = KafkaReceiver(
        receiverSetting().copy(
          commitStrategy = CommitStrategy.BySize(2 * count)
        )
      )
      receiver.receive(topic.name())
        .take(count)
        .collectIndexed { index, value ->
          if (index == lastIndex) {
            value.offset.acknowledge()
            receiver.committedCount(topic.name()) shouldBe 0
          } else value.offset.acknowledge()
        }

      receiver.committedCount(topic.name()) shouldBe count.toLong()
    }
  }

  test("All acknowledged messages are committed on flow failure") {
    withTopic {
      publishToKafka(topic, produced)
      val receiver = KafkaReceiver(
        receiverSetting().copy(
          commitStrategy = CommitStrategy.BySize(2 * count)
        )
      )
      val exception = shouldThrow<RuntimeException> {
        receiver.receive(topic.name())
          .collectIndexed { index, value ->
            if (index == lastIndex) {
              value.offset.acknowledge()
              receiver.committedCount(topic.name()) shouldBe 0
              throw Boom
            } else value.offset.acknowledge()
          }
      }

      exception shouldBe Boom
      receiver.committedCount(topic.name()) shouldBe count.toLong()
    }
  }

  test("All acknowledged messages are committed on flow cancellation") {
    withTopic {
      val scope = this
      publishToKafka(topic, produced)
      val receiver = KafkaReceiver(
        receiverSetting().copy(
          commitStrategy = CommitStrategy.BySize(2 * count)
        )
      )
      val latch = CompletableDeferred<Unit>()
      val job = receiver.receive(topic.name())
        .mapIndexed { index, value ->
          if (index == lastIndex) {
            value.offset.acknowledge()
            receiver.committedCount(topic.name()) shouldBe 0
            require(latch.complete(Unit)) { "Latch completed twice" }
          } else value.offset.acknowledge()
        }.launchIn(scope)

      latch.await()
      job.cancelAndJoin()

      receiver.committedCount(topic.name()) shouldBe count.toLong()
    }
  }

  test("Manual commit also commits all acknowledged offsets") {
    withTopic {
      publishToKafka(topic, produced)
      val receiver = KafkaReceiver(
        receiverSetting().copy(
          commitStrategy = CommitStrategy.BySize(2 * count)
        )
      )
      receiver.receive(topic.name())
        .take(count)
        .collectIndexed { index, value ->
          if (index == lastIndex) {
            value.offset.commit()
            receiver.committedCount(topic.name()) shouldBe count.toLong()
          } else value.offset.acknowledge()
        }
    }
  }

  test("receiveAutoAck") {
    withTopic {
      publishToKafka(topic, produced)
      val receiver = KafkaReceiver()

      receiver.receiveAutoAck(topic.name())
        .flattenConcat()
        .take(count)
        .collect()

      receiver.committedCount(topic.name()) shouldBe count.toLong()
    }
  }

  test("receiveAutoAck does not receive same records") {
    withTopic {
      publishToKafka(topic, produced)
      val receiver = KafkaReceiver()

      receiver.receiveAutoAck(topic.name())
        .flattenConcat()
        .take(count)
        .collect()

      receiver.committedCount(topic.name()) shouldBe count.toLong()

      val seconds = produced(count + 1, count + 1 + count)
      publishToKafka(topic, seconds)

      receiver.receiveAutoAck(topic.name())
        .flattenConcat()
        .map { KeyValue(it.key(), it.value()) }
        .take(count)
        .toSet() shouldBe seconds.toSet()

      receiver.committedCount(topic.name()) shouldBe (2L * count)
    }
  }
})
