package io.github.nomisrev.kafka

import io.github.nomisRev.kafka.Admin
import io.github.nomisRev.kafka.AdminSettings
import io.github.nomisRev.kafka.await
import io.github.nomisRev.kafka.createTopic
import io.github.nomisRev.kafka.deleteTopic
import io.github.nomisRev.kafka.describeTopic
import io.github.nomisRev.kafka.publisher.Acks
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.publisher.PublisherSettings
import io.github.nomisRev.kafka.publisher.TransactionalScope
import io.github.nomisRev.kafka.receiver.AutoOffsetReset
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import io.github.nomisrev.kafka.receiver.KeyValue
import io.kotest.assertions.nondeterministic.until
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import io.kotest.core.extensions.install
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

private val testIterations: Int =
  System.getProperties().getProperty("io.github.nomisrev.kafka.TEST_ITERATIONS")?.toIntOrNull() ?: 1

private val consumerPollingTimeout = 1.seconds

/** The old `runTest` timeout. */
private val topicTestTimeout = 1.minutes

/** Specs may run concurrently, so each [withTopic] gets its own topic and consumer group. */
abstract class KafkaSpec(body: KafkaSpec.() -> Unit) : FunSpec() {

  val kafka: Kafka = install(KafkaContainer.extension)

  private fun groupId(topic: NewTopic): String =
    "group-${topic.name()}"

  fun KafkaReceiver(topic: NewTopic): KafkaReceiver<String, String> =
    KafkaReceiver(receiverSetting(topic))

  fun receiverSetting(topic: NewTopic): ReceiverSettings<String, String> =
    ReceiverSettings(
      bootstrapServers = kafka.bootstrapServers,
      keyDeserializer = StringDeserializer(),
      valueDeserializer = StringDeserializer(),
      groupId = groupId(topic),
      autoOffsetReset = AutoOffsetReset.Earliest,
      pollTimeout = consumerPollingTimeout
    )

  fun publisherSettings() = PublisherSettings(
    bootstrapServers = kafka.bootstrapServers,
    keySerializer = StringSerializer(),
    valueSerializer = StringSerializer(),
    properties = Properties().apply {
      put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000.toString())
      put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000.toString())
    }
  )

  @IgnorableReturnValue
  suspend fun <A> publishScope(block: suspend TransactionalScope<String, String>.() -> A): A =
    KafkaPublisher(publisherSettings()).use { it.publishScope(block) }

  private fun adminProperties(): Properties = Properties().apply {
    put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
    put(AdminClientConfig.CLIENT_ID_CONFIG, "test-kafka-admin-client-${UUID.randomUUID()}")
    put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "10000")
  }

  fun adminSettings(): AdminSettings =
    AdminSettings(kafka.bootstrapServers, adminProperties())

  inline fun <A> admin(body: Admin.() -> A): A =
    Admin(adminSettings()).use(body)

  fun publisherSettings(
    acknowledgments: Acks = Acks.One,
    properties: Properties.() -> Unit
  ): PublisherSettings<String, String> {
    val publisherSettings = publisherSettings()
    return publisherSettings().copy(
      acknowledgments = acknowledgments,
      properties = Properties().apply {
        properties()
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, publisherSettings.bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, publisherSettings.keySerializer::class.qualifiedName)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, publisherSettings.valueSerializer::class.qualifiedName)
        put(ProducerConfig.ACKS_CONFIG, acknowledgments.value)
      }
    )
  }

  //<editor-fold desc="utilities">
  private fun nextTopicName(): String =
    "topic-${UUID.randomUUID()}"

  inner class TopicTestScope(
    val topic: NewTopic,
    scope: CoroutineScope
  ) : CoroutineScope by scope {
    fun KafkaReceiver(): KafkaReceiver<String, String> =
      KafkaReceiver(topic)

    fun receiverSetting(): ReceiverSettings<String, String> =
      receiverSetting(topic)

    fun produce(count: Int): List<ProducerRecord<String, String>> =
      produce(0 until count)

    fun produce(range: IntRange): List<ProducerRecord<String, String>> =
      range.map { createProducerRecord(it) }

    fun createProducerRecord(index: Int, partitions: Int = 4): ProducerRecord<String, String> {
      val partition: Int = index % partitions
      return ProducerRecord<String, String>(topic.name(), partition, "$index", "Message $index")
    }
  }

  suspend fun withTopic(
    topicConfig: Map<String, String> = emptyMap(),
    partitions: Int = 4,
    replicationFactor: Short = 1,
    test: suspend TopicTestScope.(NewTopic) -> Unit
  ): Unit = repeat(testIterations) {
    withTimeout(topicTestTimeout) {
      val topic = NewTopic(nextTopicName(), partitions, replicationFactor).configs(topicConfig)
      admin {
        createTopic(topic)
        awaitLeaders(topic)
        try {
          TopicTestScope(topic, this@withTimeout).test(topic)
        } finally {
          topic.shouldBeEmpty()
          deleteTopic(topic.name())
        }
      }
    }
  }

  /**
   * KRaft's `createTopic` returns before the broker hosts the partitions, and an idempotent producer retrying
   * through `NOT_LEADER_OR_FOLLOWER` can loop forever on `OutOfOrderSequenceException`. A leader in `describeTopic`
   * appears too early, so wait for the replica log.
   */
  private suspend fun Admin.awaitLeaders(topic: NewTopic): Unit =
    until(10.seconds) {
      val leaders = describeTopic(topic.name())?.partitions()?.map { partition ->
        partition.leader()?.let { TopicPartitionReplica(topic.name(), partition.partition(), it.id()) }
      }
      if (leaders == null || leaders.any { it == null }) false
      else describeReplicaLogDirs(leaders.filterNotNull()).all().await().values.all { it.currentReplicaLogDir != null }
    }

  @JvmName("publishPairsToKafka")
  suspend fun publishToKafka(
    topic: NewTopic,
    messages: Iterable<KeyValue>,
  ): Unit =
    publishToKafka(messages.map { (key, value) ->
      ProducerRecord(topic.name(), key, value)
    })

  suspend fun publishToKafka(messages: Iterable<ProducerRecord<String, String>>): Unit =
    publishScope {
      offer(messages)
    }
  //</editor-fold>

  //<editor-fold desc="Assertions">
  suspend fun <K, V> KafkaReceiver<K, V>.committedCount(topic: String): Long =
    admin {
      val description = requireNotNull(describeTopic(topic)) { "Topic $topic not found" }
      val topicPartitions = description.partitions().map {
        TopicPartition(topic, it.partition())
      }.toSet()

      withConsumer {
        committed(topicPartitions)
          .mapNotNull { (value) ->
            value?.takeIf { it.offset() > 0 }?.offset()
          }.sum()
      }
    }

  /**
   * Fails on any record past the group's committed offsets. Reads up to the end offsets, not for a time window, which
   * would pass on a slow group join. `assign` so the group is never joined or rebalanced.
   */
  suspend fun NewTopic.shouldBeEmpty() {
    val consumerProperties = Properties().apply {
      put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
      put(ConsumerConfig.GROUP_ID_CONFIG, groupId(this@shouldBeEmpty))
      put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
      put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    }
    val unconsumed = withContext(Dispatchers.IO) {
      KafkaConsumer(consumerProperties, StringDeserializer(), StringDeserializer()).use { consumer ->
        val partitions = consumer.partitionsFor(name()).map { TopicPartition(it.topic(), it.partition()) }
        consumer.assign(partitions)
        val committed = consumer.committed(partitions.toSet())
        partitions.forEach { partition ->
          committed[partition]?.let { consumer.seek(partition, it.offset()) }
            ?: consumer.seekToBeginning(listOf(partition))
        }
        val end = consumer.endOffsets(partitions)
        val found = mutableListOf<ConsumerRecord<String, String>>()
        until(10.seconds) {
          found += consumer.poll(Duration.ofMillis(100))
          partitions.all { consumer.position(it) >= end.getValue(it) }
        }
        found
      }
    }
    if (unconsumed.isNotEmpty()) throw AssertionError("Expected no unconsumed records, but found $unconsumed")
  }

  suspend infix fun NewTopic.assertHasRecord(records: ProducerRecord<String, String>) {
    KafkaReceiver(this)
      .receive(name())
      .map {
        it.apply { offset.acknowledge() }
      }.take(1)
      .map { it.value() }
      .toList() shouldBe listOf(records.value())
    shouldBeEmpty()
  }

  suspend infix fun NewTopic.assertHasRecords(records: Iterable<ProducerRecord<String, String>>) {
    KafkaReceiver(this)
      .receive(name())
      .map { record ->
        record
          .also { record.offset.acknowledge() }
      }
      .take(records.toList().size)
      .toList()
      .groupBy({ it.partition() }) { it.value() } shouldBe records.groupBy({ it.partition() }) { it.value() }
    shouldBeEmpty()
  }

  @JvmName("shouldHaveAllRecords")
  suspend infix fun NewTopic.assertHasRecords(
    records: Iterable<Iterable<ProducerRecord<String, String>>>
  ) {
    val expected =
      records.flatten().groupBy({ it.partition() }) { it.value() }.mapValues { it.value.toSet() }
    KafkaReceiver(this)
      .receive(name())
      .map { record ->
        record.also { record.offset.acknowledge() }
      }
      .take(records.flatten().size)
      .toList()
      .groupBy({ it.partition() }) { it.value() }
      .mapValues { it.value.toSet() } shouldBe expected
    shouldBeEmpty()
  }

  //</editor-fold>
  //<editor-fold desc="Description">
  fun stubProducer(failOnNumber: Int? = null): suspend (PublisherSettings<String, String>) -> Producer<String, String> =
    {
      val producer = KafkaProducer(it.properties(), it.keySerializer, it.valueSerializer)
      object : Producer<String, String> {
        override fun clientInstanceId(p0: Duration?): Uuid =
          producer.clientInstanceId(p0)

        override fun close() {}

        override fun close(timeout: Duration?) {}

        override fun initTransactions() =
          producer.initTransactions()

        override fun beginTransaction() =
          producer.beginTransaction()

        @Suppress("OVERRIDE_DEPRECATION")
        override fun sendOffsetsToTransaction(
          offsets: MutableMap<TopicPartition, OffsetAndMetadata>?,
          consumerGroupId: String?
        ) = producer.sendOffsetsToTransaction(offsets, ConsumerGroupMetadata(consumerGroupId))

        override fun sendOffsetsToTransaction(
          offsets: MutableMap<TopicPartition, OffsetAndMetadata>?,
          groupMetadata: ConsumerGroupMetadata?
        ) = producer.sendOffsetsToTransaction(offsets, groupMetadata)

        override fun commitTransaction() =
          producer.commitTransaction()

        override fun abortTransaction() =
          producer.abortTransaction()

        override fun flush() =
          producer.flush()

        override fun partitionsFor(topic: String?): MutableList<PartitionInfo> =
          producer.partitionsFor(topic)

        override fun metrics(): MutableMap<MetricName, out Metric> =
          producer.metrics()

        override fun send(record: ProducerRecord<String, String>, callback: Callback): Future<RecordMetadata> =
          if (failOnNumber != null && record.key() == failOnNumber.toString()) {
            Executors.newScheduledThreadPool(1).schedule(
              {
                callback.onCompletion(null, Boom)
              },
              50,
              TimeUnit.MILLISECONDS
            )

            CompletableFuture.supplyAsync { throw AssertionError("Should never be called") }
          } else producer.send(record, callback)

        override fun send(record: ProducerRecord<String, String>): Future<RecordMetadata> =
          producer.send(record)
      }
    }
  //</editor-fold>

  init {
    body()
  }
}

@Suppress("ObjectInheritsException", "unused")
object Boom : RuntimeException("Boom!") {
  private fun readResolve(): Any = Boom
}
