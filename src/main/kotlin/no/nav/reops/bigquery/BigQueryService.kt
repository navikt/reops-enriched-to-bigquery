package no.nav.reops.bigquery

import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.Exceptions
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter
import com.google.cloud.bigquery.storage.v1.TableName
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PreDestroy
import no.nav.reops.event.EnrichedEvent
import no.nav.reops.event.WebsiteChangeEvent
import org.json.JSONArray
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException

/**
 * Writes events to BigQuery using the Storage Write API with the DEFAULT stream.
 *
 * Design decisions:
 * - DEFAULT stream (at-least-once): eliminates CreateWriteStream quota cost (10,000/hour limit).
 *   Kafka batch semantics are already at-least-once, so no regression in delivery guarantees.
 * - Long-lived JsonStreamWriters: one per table, initialized at startup, reused across all batches.
 *   Google docs explicitly discourage short-lived connections.
 * - Parallel table writes: all tables appended concurrently via CompletableFuture.
 *   Wall time = max(table latency), not sum.
 * - Async append: futures collected and awaited batch-wide, not per-append.
 *   Allows the gRPC bidirectional stream to pipeline requests.
 * - Chunking: rows split into ≤MAX_ROWS_PER_CHUNK chunks, each ≤MAX_BYTES_PER_CHUNK bytes.
 *   Guards against the 20 MB AppendRows hard limit.
 */
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
    meterRegistry: MeterRegistry,
    writersOverride: Map<String, JsonStreamWriter>? = null,
) {
    private val bigqueryRequestsSuccess: Counter =
        meterRegistry.counter("bigquery_requests_total", "result", "success")
    private val bigqueryRequestsFailure: Counter =
        meterRegistry.counter("bigquery_requests_total", "result", "failure")
    private val bigqueryRowsWritten: Counter =
        meterRegistry.counter("bigquery_rows_written_total")
    private val bigqueryChunksTotal: Counter =
        meterRegistry.counter("bigquery_chunks_total")

    private val writers: Map<String, JsonStreamWriter> by lazy {
        writersOverride ?: run {
            val tables = listOf(websiteEventTable, eventDataTable, sessionTable, sessionDataTable, websiteTable)
            LOG.info(
                "Initializing {} long-lived BigQuery DEFAULT stream writers for dataset {}.{}",
                tables.size, projectId, datasetName
            )
            tables.associateWith { table ->
                val tablePath = TableName.of(projectId, datasetName, table)
                val streamName = "${tablePath}/_default"
                LOG.info("Creating writer for stream: {}", streamName)
                JsonStreamWriter.newBuilder(streamName, bigQueryWriteClient).build().also {
                    LOG.info("Writer ready for table: {}", table)
                }
            }
        }
    }

    fun sendEvents(events: List<EnrichedEvent>) {
        if (events.isEmpty()) return

        LOG.info("Processing batch of {} enriched events", events.size)

        try {
            val websiteEventRows = JSONArray()
            val eventDataRows = JSONArray()
            val sessionRows = JSONArray()
            val sessionDataRows = JSONArray()
            var newSessionCount = 0

            for (event in events) {
                websiteEventRows.put(RowMapper.toWebsiteEventRow(event))
                RowMapper.toEventDataRows(event).forEach { eventDataRows.put(it) }
                if (event.isNewSession) {
                    newSessionCount++
                    sessionRows.put(RowMapper.toSessionRow(event))
                    RowMapper.toSessionDataRows(event).forEach { sessionDataRows.put(it) }
                }
            }

            LOG.debug(
                "Row counts — website_event: {}, event_data: {}, session: {}, session_data: {} ({} new sessions)",
                websiteEventRows.length(), eventDataRows.length(),
                sessionRows.length(), sessionDataRows.length(), newSessionCount
            )

            val writes = buildList {
                add(websiteEventTable to websiteEventRows)
                if (eventDataRows.length() > 0) add(eventDataTable to eventDataRows)
                if (sessionRows.length() > 0) add(sessionTable to sessionRows)
                if (sessionDataRows.length() > 0) add(sessionDataTable to sessionDataRows)
            }

            executeParallelWrites(writes)

            bigqueryRequestsSuccess.increment()
            bigqueryRowsWritten.increment(events.size.toDouble())
            LOG.info(
                "Successfully wrote batch of {} events to {} tables ({} new sessions)",
                events.size, writes.size, newSessionCount
            )
        } catch (ex: Exception) {
            bigqueryRequestsFailure.increment()
            LOG.error("Failed to write batch of {} events to BigQuery", events.size, ex)
            throw ex
        }
    }

    fun sendWebsiteEvents(events: List<WebsiteChangeEvent>) {
        if (events.isEmpty()) return

        LOG.info("Processing batch of {} website change events", events.size)

        try {
            val rows = JSONArray()
            for (event in events) rows.put(RowMapper.toWebsiteRow(event))

            executeParallelWrites(listOf(websiteTable to rows))

            bigqueryRequestsSuccess.increment()
            bigqueryRowsWritten.increment(events.size.toDouble())
            LOG.info("Successfully wrote {} website change events to BigQuery", events.size)
        } catch (ex: Exception) {
            bigqueryRequestsFailure.increment()
            LOG.error("Failed to write {} website change events to BigQuery", events.size, ex)
            throw ex
        }
    }

    /**
     * Fires all table writes in parallel. Each write is chunked internally.
     * Awaits all futures before returning — ensures Kafka offsets are only
     * committed after all data is durably written.
     */
    private fun executeParallelWrites(writes: List<Pair<String, JSONArray>>) {
        val futures = writes.flatMap { (table, rows) ->
            chunkRows(table, rows).map { chunk ->
                appendChunkAsync(table, chunk)
            }
        }

        LOG.debug("Awaiting {} append futures across {} tables", futures.size, writes.size)

        val errors = mutableListOf<Throwable>()
        futures.forEach { future ->
            try {
                future.get()
            } catch (ex: ExecutionException) {
                errors.add(ex.cause ?: ex)
            } catch (ex: Exception) {
                errors.add(ex)
            }
        }

        if (errors.isNotEmpty()) {
            val combined = errors.first()
            errors.drop(1).forEach { combined.addSuppressed(it) }
            throw RuntimeException("One or more BigQuery appends failed (${errors.size} errors)", combined)
        }
    }

    private fun chunkRows(table: String, rows: JSONArray): List<JSONArray> {
        if (rows.length() == 0) return emptyList()

        val chunks = mutableListOf<JSONArray>()
        var current = JSONArray()
        var currentBytes = 0L

        for (i in 0 until rows.length()) {
            val row = rows.getJSONObject(i)
            val rowBytes = row.toString().length.toLong()

            val wouldExceedCount = current.length() >= MAX_ROWS_PER_CHUNK
            val wouldExceedBytes = currentBytes + rowBytes > MAX_BYTES_PER_CHUNK

            if (current.length() > 0 && (wouldExceedCount || wouldExceedBytes)) {
                LOG.debug("Chunk boundary for {}: {} rows, {} bytes", table, current.length(), currentBytes)
                chunks.add(current)
                current = JSONArray()
                currentBytes = 0L
            }

            current.put(row)
            currentBytes += rowBytes
        }

        if (current.length() > 0) chunks.add(current)

        if (chunks.size > 1) {
            LOG.info("Table {} split into {} chunks ({} total rows)", table, chunks.size, rows.length())
        }

        return chunks
    }

    private fun appendChunkAsync(table: String, chunk: JSONArray): CompletableFuture<Void> {
        val writer = writers[table]
            ?: throw IllegalStateException("No writer initialized for table: $table")

        bigqueryChunksTotal.increment()
        LOG.debug("Appending chunk of {} rows to {}", chunk.length(), table)

        val apiFuture = try {
            writer.append(chunk)
        } catch (ex: Exception) {
            LOG.error("Failed to submit append for table {}: {}", table, ex.message, ex)
            return CompletableFuture<Void>().also { it.completeExceptionally(ex) }
        }

        val result = CompletableFuture<Void>()

        apiFuture.addListener({
            try {
                val response = apiFuture.get()
                if (response.hasError()) {
                    val err = response.error
                    LOG.error(
                        "BigQuery append error for table {}: code={} message={}",
                        table, err.code, err.message
                    )
                    result.completeExceptionally(
                        RuntimeException("BigQuery append error for $table: ${err.code} ${err.message}")
                    )
                } else {
                    if (response.hasUpdatedSchema()) {
                        LOG.info("Schema update detected for table {} — writer will reconnect automatically", table)
                    }
                    LOG.debug("Append acknowledged for table {}: offset={}", table, response.appendResult.offset.value)
                    result.complete(null)
                }
            } catch (ex: ExecutionException) {
                val cause = ex.cause ?: ex
                if (cause is Exceptions.AppendSerializationError) {
                    LOG.error(
                        "Serialization errors for table {} — row-level errors: {}",
                        table, cause.rowIndexToErrorMessage
                    )
                }
                LOG.error("Append failed for table {}: {}", table, cause.message, cause)
                result.completeExceptionally(cause)
            } catch (ex: Exception) {
                LOG.error("Unexpected error in append callback for table {}: {}", table, ex.message, ex)
                result.completeExceptionally(ex)
            }
        }, Runnable::run)

        return result
    }

    @PreDestroy
    fun closeWriters() {
        LOG.info("Shutting down {} BigQuery stream writers", writers.size)
        writers.forEach { (table, writer) ->
            try {
                writer.close()
                LOG.info("Writer closed for table: {}", table)
            } catch (ex: Exception) {
                LOG.warn("Error closing writer for table {}: {}", table, ex.message)
            }
        }
    }

    private companion object {
        private val LOG = LoggerFactory.getLogger(BigQueryService::class.java)

        /** Max rows per AppendRows request. BQ recommends 500–1,000. */
        private const val MAX_ROWS_PER_CHUNK = 1_000

        /** Max bytes per AppendRows request. Hard limit is 20 MB; we stay at 18 MB. */
        private const val MAX_BYTES_PER_CHUNK = 18 * 1024 * 1024L
    }
}
