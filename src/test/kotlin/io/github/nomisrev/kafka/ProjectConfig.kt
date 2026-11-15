package io.github.nomisrev.kafka

import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.test.TestCaseOrder
import io.kotest.engine.concurrency.SpecExecutionMode

/** Our own property: Kotest only has one for spec order. */
private fun String.toTestCaseOrder(): TestCaseOrder =
  TestCaseOrder.entries.firstOrNull { it.name.equals(this, ignoreCase = true) }
    ?: error(
      "kotest.test.order must be one of ${TestCaseOrder.entries.joinToString("/") { it.name.lowercase() }}, " +
        "was \"$this\""
    )

/** `LimitedConcurrency(-1)` would admit nothing. */
private fun String.toSpecConcurrency(): Int =
  toIntOrNull()?.takeIf { it >= 0 }
    ?: error("kotest.spec.concurrency must be a non-negative integer (0 unbounded, 1 sequential), was \"$this\"")

/** Override with `-Dkotest.spec.concurrency=N` and `-Dkotest.test.order`. Tests in a spec always run sequentially. */
class ProjectConfig : AbstractProjectConfig() {
  override val specExecutionMode: SpecExecutionMode =
    when (val configured = System.getProperty("kotest.spec.concurrency")?.toSpecConcurrency()) {
      null, 1 -> SpecExecutionMode.Sequential
      0 -> SpecExecutionMode.Concurrent
      else -> SpecExecutionMode.LimitedConcurrency(configured)
    }

  override val testCaseOrder: TestCaseOrder =
    System.getProperty("kotest.test.order")?.toTestCaseOrder() ?: TestCaseOrder.Sequential
}
