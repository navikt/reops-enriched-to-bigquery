package no.nav.reops.bigquery

import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest
import com.google.cloud.bigquery.storage.v1.Exceptions
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter
import com.google.cloud.bigquery.storage.v1.TableName
import com.google.cloud.bigquery.storage.v1.WriteStream
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import no.nav.reops.event.EnrichedEvent
import no.nav.reops.event.WebsiteChangeEvent
import org.json.JSONArray
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service
class BigQueryService(
    private val bigQueryWriteClient: BigQueryWriteClient,
    @Value("\${reops.bigquery.project}") private val projectId: String,
    @Value("\${reops.bigquery.dataset}") private val datasetName: String,
    @Value("\${reops.bigquery.tables.website-event}") private val websiteEventTable: String,
    @Value("\${reops.bigquery.tables.event-data}") private val eventDataTable: String,
    @Value("\${reops.bigquery.tables.session}") private val sessionTable: String,
    @Value("\${reops.bigquery.tables.session-data}") private val sessionDataTable: String,
    @Value("\${reops.bigquery.tables.website}") private val websiteTable: String,
    meterRegistry: MeterRegistry
) {
    private val bigqueryRequestsSuccess: Counter =
        meterRegistry.counter("bigquery_requests_total", "result", "success")
    private val bigqueryRequestsFailure: Counter =
        meterRegistry.counter("bigquery_requests_total", "result", "failure")
    private val bigqueryRowsWritten: Counter =
        meterRegistry.counter("bigquery_rows_written_total")

    fun sendEvents(events: List<EnrichedEvent>) {
        if (events.isEmpty()) return

        try {
            val websiteEventRows = JSONArray()
            val eventDataRows = JSONArray()
            val sessionRows = JSONArray()
            val sessionDataRows = JSONArray()

            for (event in events) {
                websiteEventRows.put(RowMapper.toWebsiteEventRow(event))
                RowMapper.toEventDataRows(event).forEach { eventDataRows.put(it) }
                if (event.isNewSession) {
                    sessionRows.put(RowMapper.toSessionRow(event))
                    RowMapper.toSessionDataRows(event).forEach { sessionDataRows.put(it) }
                }
            }

            writeToTable(websiteEventTable, websiteEventRows)
            if (eventDataRows.length() > 0) writeToTable(eventDataTable, eventDataRows)
            if (sessionRows.length() > 0) writeToTable(sessionTable, sessionRows)
            if (sessionDataRows.length() > 0) writeToTable(sessionDataTable, sessionDataRows)

            bigqueryRequestsSuccess.increment()
            bigqueryRowsWritten.increment(events.size.toDouble())
            LOG.info("Committed {} events to BigQuery (4 tables)", events.size)
        } catch (ex: Exception) {
            bigqueryRequestsFailure.increment()
            LOG.error("Failed to write {} events to BigQuery", events.size, ex)
            throw ex
        }
    }

    fun sendWebsiteEvents(events: List<WebsiteChangeEvent>) {
        if (events.isEmpty()) return

        try {
            val rows = JSONArray()
            for (event in events) {
                rows.put(RowMapper.toWebsiteRow(event))
            }
            writeToTable(websiteTable, rows)

            bigqueryRequestsSuccess.increment()
            bigqueryRowsWritten.increment(events.size.toDouble())
            LOG.info("Committed {} website events to BigQuery", events.size)
        } catch (ex: Exception) {
            bigqueryRequestsFailure.increment()
            LOG.error("Failed to write {} website events to BigQuery", events.size, ex)
            throw ex
        }
    }

    private fun writeToTable(table: String, rows: JSONArray) {
        val tablePath = TableName.of(projectId, datasetName, table)
        LOG.debug("Writing {} rows to {}", rows.length(), tablePath)

        val writeStream = try {
            bigQueryWriteClient.createWriteStream(
                CreateWriteStreamRequest.newBuilder()
                    .setParent(tablePath.toString())
                    .setWriteStream(WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build())
                    .build()
            )
        } catch (ex: Exception) {
            throw RuntimeException("Failed to create write stream for $table ($tablePath)", ex)
        }

        try {
            JsonStreamWriter.newBuilder(writeStream.name, writeStream.tableSchema, bigQueryWriteClient).build().use { writer ->
                val response = writer.append(rows).get()
                if (response.hasError()) {
                    throw RuntimeException(
                        "BigQuery append failed for $table: ${response.error.code} ${response.error.message}"
                    )
                }
            }
        } catch (ex: java.util.concurrent.ExecutionException) {
            val cause = ex.cause ?: ex
            if (cause is Exceptions.AppendSerializationError) {
                val rowErrors = cause.rowIndexToErrorMessage
                LOG.error("BigQuery row-level errors for {}: {}", table, rowErrors)
            }
            throw RuntimeException("BigQuery append failed for $table: ${cause.message}", cause)
        }

        bigQueryWriteClient.finalizeWriteStream(writeStream.name)

        val commitResponse = bigQueryWriteClient.batchCommitWriteStreams(
            BatchCommitWriteStreamsRequest.newBuilder()
                .setParent(tablePath.toString())
                .addWriteStreams(writeStream.name)
                .build()
        )

        if (!commitResponse.hasCommitTime()) {
            val errors = commitResponse.streamErrorsList.joinToString { "${it.code}: ${it.errorMessage}" }
            throw RuntimeException("BigQuery batch commit failed for $table: $errors")
        }
    }

    private companion object {
        private val LOG = LoggerFactory.getLogger(BigQueryService::class.java)
    }
}
