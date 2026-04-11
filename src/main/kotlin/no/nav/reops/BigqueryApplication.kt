package no.nav.reops

import org.slf4j.bridge.SLF4JBridgeHandler
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.util.logging.Level
import java.util.logging.Logger

@SpringBootApplication
class BigqueryApplication

fun main(args: Array<String>) {
    SLF4JBridgeHandler.removeHandlersForRootLogger() // Fjern root logger så den brukes av logback i stedet for java.util.logging
    SLF4JBridgeHandler.install()
    Logger.getLogger("com.google.cloud.bigquery.storage.v1").level = Level.WARNING
    runApplication<BigqueryApplication>(*args)
}