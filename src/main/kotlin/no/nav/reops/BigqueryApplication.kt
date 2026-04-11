package no.nav.reops

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.util.logging.Level
import java.util.logging.Logger

@SpringBootApplication
class BigqueryApplication

fun main(args: Array<String>) {
    Logger.getLogger("com.google.cloud.bigquery.storage.v1").level = Level.WARNING
    runApplication<BigqueryApplication>(*args)
}