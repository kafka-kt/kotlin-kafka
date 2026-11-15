package io.github.nomisrev.kafka.publisher

import io.github.nomisRev.kafka.publisher.produce
import io.github.nomisRev.kafka.publisher.produceOrThrow
import io.github.nomisrev.kafka.Boom
import io.github.nomisrev.kafka.KafkaSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.toList
import org.apache.kafka.clients.producer.RecordMetadata

class FlowProduceSpec : KafkaSpec({
  test("produce - All offered messages are received") {
    withTopic {
      val count = 10_000
      val records = produce(count)
        .asFlow()
        .flowOn(Dispatchers.IO)

      records
        .produce(publisherSettings())
        .flowOn(Dispatchers.Default)
        .collect()

      topic.assertHasRecords(records.toList())
    }
  }

  test("produce - Failure does not stop previous send messages") {
    withTopic {
      var count = 0
      val records = produce(10)
      val flow = records
        .asFlow()
        .flowOn(Dispatchers.IO)
        .append { throw Boom }

      val e = runCatching {
        flow
          .produce(publisherSettings())
          .flowOn(Dispatchers.Default)
          .onEach { count++ }
          .collect()
      }.exceptionOrNull()

      count shouldBe records.size
      e shouldBe Boom
      topic.assertHasRecords(records)
    }
  }

  test("produce - Can handle exception from upstream") {
    withTopic {
      val records = produce(0..4)
      val records2 = produce(5..10)
      val allRecords = records + records2

      val flow = records
        .asFlow()
        .flowOn(Dispatchers.IO)
        .append { throw Boom }
        .catch { emitAll(records2) }

      val count = flow
        .produce(publisherSettings())
        .flowOn(Dispatchers.Default)
        .toList()

      count.size shouldBe allRecords.size
      topic.assertHasRecords(allRecords)
    }
  }

  test("produce - Can handle exception from producer") {
    withTopic {
      val buffer = mutableListOf<Result<RecordMetadata>>()
      val records = produce(10)

      val flow = records
        .asFlow()
        .flowOn(Dispatchers.IO)

      val result = runCatching {
        flow
          .produce(publisherSettings().copy(createProducer = stubProducer(failOnNumber = 5)))
          .flowOn(Dispatchers.Default)
          .onEach { buffer.add(it) }
          .collect()
      }

      buffer.size shouldBe records.size
      buffer.count { it.isFailure } shouldBe 1
      result.exceptionOrNull() shouldBe Boom
      topic.assertHasRecords(records.toMutableList().apply { removeAt(5) })
    }
  }

  test("produce - Can catch exception from producer") {
    withTopic {
      val error = CompletableDeferred<Throwable>()
      val records = produce(10)

      val flow = records
        .asFlow()
        .flowOn(Dispatchers.IO)

      flow
        .produce(publisherSettings().copy(createProducer = stubProducer(failOnNumber = 5)))
        .catch { require(error.complete(it)) { "Only 1 error expected." } }
        .flowOn(Dispatchers.Default)
        .toList()

      error.await() shouldBe Boom
      topic.assertHasRecords(records.toMutableList().apply { removeAt(5) })
    }
  }

  test("produceOrThrow - All offered messages are received") {
    withTopic {
      val records = produce(10)
        .asFlow()
        .flowOn(Dispatchers.IO)

      records
        .produceOrThrow(publisherSettings())
        .flowOn(Dispatchers.Default)
        .collect()

      topic.assertHasRecords(records.toList())
    }
  }

  test("produceOrThrow - Failure does not stop previous send messages") {
    withTopic {
      val records = produce(10)
      val flow = records
        .asFlow()
        .flowOn(Dispatchers.IO)
        .append { throw Boom }

      val result = runCatching {
        flow
          .produceOrThrow(publisherSettings())
          .flowOn(Dispatchers.Default)
          .collect()
      }

      result.exceptionOrNull() shouldBe Boom
      topic.assertHasRecords(records)
    }
  }

  test("produceOrThrow - Can catch exception from producer") {
    withTopic {
      val error = CompletableDeferred<Throwable>()
      val records = produce(10)

      val flow = records
        .asFlow()
        .flowOn(Dispatchers.IO)

      flow
        .produceOrThrow(publisherSettings().copy(createProducer = stubProducer(failOnNumber = 5)))
        .catch { require(error.complete(it)) { "Only 1 error expected." } }
        .flowOn(Dispatchers.Default)
        .toList()

      error.await() shouldBe Boom
      topic.assertHasRecords(records.toMutableList().apply { removeAt(5) })
    }
  }

  xtest("stressSlow") {
    withTopic {
      val records = produce(10)

      val flow = records.asFlow()
        .onEach { delay(25_000) }
        .flowOn(Dispatchers.IO)

      flow
        .produce(publisherSettings())
        .flowOn(Dispatchers.Default)
        .collect()

      topic.assertHasRecords(records)
    }
  }
})

private suspend fun <T> FlowCollector<T>.emitAll(iterable: Iterable<T>) =
  iterable.forEach { emit(it) }

private fun <A> Flow<A>.append(block: suspend FlowCollector<A>.() -> Unit): Flow<A> =
  flow {
    collect { emit(it) }
    block()
  }
