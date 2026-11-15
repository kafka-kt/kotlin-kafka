package io.github.nomisrev.kafka.receiver

import io.github.nomisRev.kafka.receiver.CommitStrategy
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlin.time.Duration.Companion.seconds

class CommitStrategySpec : FunSpec({
  test("Negative or zero sized BySize strategy fails") {
    val actual = shouldThrow<IllegalArgumentException> {
      val _ = CommitStrategy.BySize(0)
    }.message
    actual shouldBe "Size based auto-commit requires positive non-zero commit batch size but found 0"
  }

  test("Negative or zero sized BySizeOrTime strategy fails") {
    val actual = shouldThrow<IllegalArgumentException> {
      val _ = CommitStrategy.BySizeOrTime(0, 1.seconds)
    }.message
    actual shouldBe "Size based auto-commit requires positive non-zero commit batch size but found 0"
  }

  test("Negative or zero duration BySizeOrTime strategy fails") {
    val actual = shouldThrow<IllegalArgumentException> {
      val _ = CommitStrategy.BySizeOrTime(1, 0.seconds)
    }.message
    actual shouldBe "Time based auto-commit requires positive non-zero interval but found ${0.seconds}"
  }

  test("Negative or zero duration ByTime strategy fails") {
    val actual = shouldThrow<IllegalArgumentException> {
      val _ = CommitStrategy.ByTime(0.seconds)
    }.message
    actual shouldBe "Time based auto-commit requires positive non-zero interval but found ${0.seconds}"
  }
})
