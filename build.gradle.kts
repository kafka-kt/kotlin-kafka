import kotlinx.knit.KnitPluginExtension
import org.gradle.api.tasks.testing.logging.TestExceptionFormat.*
import org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED
import org.gradle.api.tasks.testing.logging.TestLogEvent.SKIPPED
import org.gradle.api.tasks.testing.logging.TestLogEvent.STANDARD_ERROR
import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.dokka)
  alias(libs.plugins.spotless)
  alias(libs.plugins.knit)
  alias(libs.plugins.publish)
}

repositories {
  mavenCentral()
}

group = "io.github.nomisrev"

dependencies {
  api(libs.kotlinx.coroutines.core)
  api(libs.kafka.clients)
  implementation(libs.slf4j.api)

  // Kotest's runner asks for an older JUnit 6.0.x; align every module with the launcher.
  testImplementation(platform(libs.junit.bom))
  testRuntimeOnly(libs.junit.platform.launcher)
  testImplementation(libs.kotest.runner.junit6)
  testImplementation(libs.kotest.assertions.core)
  testImplementation(libs.kotest.extensions.testcontainers)
  testImplementation(libs.testcontainers.kafka)
  testImplementation(libs.slf4j.simple)
}

configure<KnitPluginExtension> {
  siteRoot = "https://nomisrev.github.io/kotlin-kafka/"
}

// 17 for Kotest's JUnit 6 runner; the library still targets Java 8 (see compileKotlin/compileJava).
configure<JavaPluginExtension> {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(17))
  }
}

kotlin {
  explicitApi()
  compilerOptions {
    languageVersion.set(KotlinVersion.KOTLIN_2_2)
    apiVersion.set(KotlinVersion.KOTLIN_2_2)
    freeCompilerArgs.addAll("-Xreturn-value-checker=full", "-Xname-based-destructuring=complete")
  }
}

tasks {
  withType<DokkaTask>().configureEach {
    outputDirectory.set(rootDir.resolve("docs"))
    moduleName.set("kotlin-kafka")
    dokkaSourceSets {
      named("main") {
        includes.from("README.md")
        perPackageOption {
          matchingRegex.set(".*\\.internal.*")
          suppress.set(true)
        }
        sourceLink {
          localDirectory.set(file("src/main/kotlin"))
          remoteUrl.set(uri("https://github.com/nomisRev/kotlin-kafka/tree/main/src/main/kotlin").toURL())
          remoteLineSuffix.set("#L")
        }
      }
    }
  }

  getByName("knitPrepare").dependsOn(getTasksByName("dokka", true))

  compileKotlin {
    compilerOptions {
      jvmTarget.set(JvmTarget.JVM_1_8)
      freeCompilerArgs.add("-Xjdk-release=1.8")
    }
  }

  compileJava {
    options.release.set(8)
  }

  withType<Test>().configureEach {
    useJUnitPlatform()
    // One test JVM shares one broker; spec concurrency is set in ProjectConfig.
    project.findProperty("stressTest")?.let { stressTest ->
      systemProperty("io.github.nomisrev.kafka.TEST_ITERATIONS", stressTest)
    }

    // A Gradle `-D` only reaches the daemon, not the test JVM.
    fun forward(name: String, expected: String, valid: (String) -> Boolean) =
      providers.systemProperty(name).orNull?.let {
        require(valid(it)) { "-D$name must be $expected, was \"$it\"" }
        systemProperty(name, it)
      }

    forward("kotest.spec.concurrency", "a non-negative integer (0 unbounded, 1 sequential)") {
      (it.toIntOrNull() ?: -1) >= 0
    }
    forward("kotest.test.order", "one of sequential/random/lexicographic") {
      it.lowercase() in setOf("sequential", "random", "lexicographic")
    }
    testLogging {
      exceptionFormat = FULL
      events = setOf(SKIPPED, FAILED, STANDARD_ERROR)
    }
  }
}
