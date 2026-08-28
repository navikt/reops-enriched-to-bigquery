package no.nav.reops.bigquery

import com.google.api.core.ApiFutures
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.Exceptions.AppendSerializationError
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter
import com.google.protobuf.Int64Value
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.nav.reops.event.EnrichedEvent
import no.nav.reops.event.EventDataEntry
import no.nav.reops.event.Session
import no.nav.reops.event.SessionDataEntry
import no.nav.reops.event.Website
import no.nav.reops.event.WebsiteChangeEvent
import no.nav.reops.event.WebsiteEvent
import no.nav.reops.event.WebsiteEventStatus
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.json.JSONArray
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Instant
import java.util.UUID

class BigQueryServiceTest {

    private lateinit var websiteEventWriter: JsonStreamWriter
    private lateinit var eventDataWriter: JsonStreamWriter
    private lateinit var sessionWriter: JsonStreamWriter
    private lateinit var sessionDataWriter: JsonStreamWriter
    private lateinit var websiteWriter: JsonStreamWriter
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var service: BigQueryService

    @BeforeEach
    fun setUp() {
        websiteEventWriter = mock()
        eventDataWriter = mock()
        sessionWriter = mock()
        sessionDataWriter = mock()
        websiteWriter = mock()
        meterRegistry = SimpleMeterRegistry()

        service = BigQueryService(
            bigQueryWriteClient = mock<BigQueryWriteClient>(),
            projectId = "test-project",
            datasetName = "test-dataset",
            websiteEventTable = "website_event",
            eventDataTable = "event_data",
            sessionTable = "session",
            sessionDataTable = "session_data",
            websiteTable = "website",
            meterRegistry = meterRegistry,
            writersOverride = mapOf(
                "website_event" to websiteEventWriter,
                "event_data" to eventDataWriter,
                "session" to sessionWriter,
                "session_data" to sessionDataWriter,
                "website" to websiteWriter,
            )
        )
    }

    // ── sendEvents ────────────────────────────────────────────────────────────

    @Test
    fun `sendEvents - writes to all 4 tables when isNewSession is true`() {
        stubSuccess(websiteEventWriter, eventDataWriter, sessionWriter, sessionDataWriter)

        service.sendEvents(listOf(eventWithData(isNewSession = true)))

        verify(websiteEventWriter).append(any<JSONArray>())
        verify(eventDataWriter).append(any<JSONArray>())
        verify(sessionWriter).append(any<JSONArray>())
        verify(sessionDataWriter).append(any<JSONArray>())
    }

    @Test
    fun `sendEvents - skips session row but writes session_data when isNewSession is false and session data present`() {
        stubSuccess(websiteEventWriter, eventDataWriter, sessionDataWriter)

        service.sendEvents(listOf(eventWithData(isNewSession = false)))

        verify(websiteEventWriter).append(any<JSONArray>())
        verify(eventDataWriter).append(any<JSONArray>())
        verify(sessionWriter, never()).append(any<JSONArray>())
        verify(sessionDataWriter).append(any<JSONArray>())
    }

    @Test
    fun `sendEvents - skips both session tables when isNewSession is false and no session data`() {
        stubSuccess(websiteEventWriter)

        service.sendEvents(listOf(eventWithoutData(isNewSession = false)))

        verify(websiteEventWriter).append(any<JSONArray>())
        verify(sessionWriter, never()).append(any<JSONArray>())
        verify(sessionDataWriter, never()).append(any<JSONArray>())
    }

    @Test
    fun `sendEvents - skips event_data table when event has no eventData`() {
        stubSuccess(websiteEventWriter, sessionWriter, sessionDataWriter)

        service.sendEvents(listOf(eventWithoutData(isNewSession = true)))

        verify(websiteEventWriter).append(any<JSONArray>())
        verify(eventDataWriter, never()).append(any<JSONArray>())
        verify(sessionWriter).append(any<JSONArray>())
    }

    @Test
    fun `sendEvents - empty list does not call any writer`() {
        service.sendEvents(emptyList())

        verify(websiteEventWriter, never()).append(any<JSONArray>())
        verify(eventDataWriter, never()).append(any<JSONArray>())
        verify(sessionWriter, never()).append(any<JSONArray>())
        verify(sessionDataWriter, never()).append(any<JSONArray>())
    }

    @Test
    fun `sendEvents - increments success counter on happy path`() {
        stubSuccess(websiteEventWriter)

        service.sendEvents(listOf(eventWithoutData(isNewSession = false)))

        val success = meterRegistry.find("bigquery_requests_total").tag("result", "success").counter()
        val failure = meterRegistry.find("bigquery_requests_total").tag("result", "failure").counter()
        assertThat(success?.count()).isEqualTo(1.0)
        assertThat(failure?.count()).isEqualTo(0.0)
    }

    @Test
    fun `sendEvents - increments failure counter and rethrows when append fails`() {
        whenever(websiteEventWriter.append(any<JSONArray>())).thenReturn(
            ApiFutures.immediateFailedFuture(RuntimeException("BQ unavailable"))
        )

        assertThatThrownBy {
            service.sendEvents(listOf(eventWithoutData(isNewSession = false)))
        }.isInstanceOf(RuntimeException::class.java)
            .hasMessageContaining("appends failed")

        val failure = meterRegistry.find("bigquery_requests_total").tag("result", "failure").counter()
        assertThat(failure?.count()).isEqualTo(1.0)
    }

    @Test
    fun `sendEvents - collects all failures when multiple tables fail`() {
        val error1 = RuntimeException("website_event failed")
        val error2 = RuntimeException("event_data failed")
        whenever(websiteEventWriter.append(any<JSONArray>())).thenReturn(ApiFutures.immediateFailedFuture(error1))
        whenever(eventDataWriter.append(any<JSONArray>())).thenReturn(ApiFutures.immediateFailedFuture(error2))
        whenever(sessionDataWriter.append(any<JSONArray>())).thenReturn(ApiFutures.immediateFailedFuture(error2))

        assertThatThrownBy {
            service.sendEvents(listOf(eventWithData(isNewSession = false)))
        }.isInstanceOf(RuntimeException::class.java)
            .satisfies({ ex ->
                assertThat(ex.cause!!.suppressed).hasSize(2)
            })
    }

    @Test
    fun `sendEvents - response with error field throws`() {
        val errorResponse = AppendRowsResponse.newBuilder()
            .setError(com.google.rpc.Status.newBuilder().setCode(3).setMessage("INVALID_ARGUMENT").build())
            .build()
        whenever(websiteEventWriter.append(any<JSONArray>())).thenReturn(ApiFutures.immediateFuture(errorResponse))

        assertThatThrownBy {
            service.sendEvents(listOf(eventWithoutData(isNewSession = false)))
        }.isInstanceOf(RuntimeException::class.java)
            .cause()
            .hasMessageContaining("INVALID_ARGUMENT")
    }

    @Test
    fun `sendEvents - processes batch of multiple events`() {
        stubSuccess(websiteEventWriter)

        service.sendEvents(listOf(
            eventWithoutData(isNewSession = false),
            eventWithoutData(isNewSession = false),
            eventWithoutData(isNewSession = false),
        ))

        verify(websiteEventWriter).append(any<JSONArray>())

        val rowsWritten = meterRegistry.find("bigquery_rows_written_total").counter()
        assertThat(rowsWritten?.count()).isEqualTo(3.0)
    }

    // ── sendWebsiteEvents ─────────────────────────────────────────────────────

    @Test
    fun `sendWebsiteEvents - writes to website table only`() {
        stubSuccess(websiteWriter)

        service.sendWebsiteEvents(listOf(websiteChangeEvent()))

        verify(websiteWriter).append(any<JSONArray>())
        verify(websiteEventWriter, never()).append(any<JSONArray>())
        verify(sessionWriter, never()).append(any<JSONArray>())
    }

    @Test
    fun `sendWebsiteEvents - empty list does not call writer`() {
        service.sendWebsiteEvents(emptyList())

        verify(websiteWriter, never()).append(any<JSONArray>())
    }

    // ── chunking ──────────────────────────────────────────────────────────────

    @Test
    fun `sendEvents - large batch exceeding MAX_ROWS_PER_CHUNK splits into multiple appends`() {
        stubSuccess(websiteEventWriter)

        val events = (1..1100).map { eventWithoutData(isNewSession = false) }
        service.sendEvents(events)

        verify(websiteEventWriter, times(2)).append(any<JSONArray>())

        val chunks = meterRegistry.find("bigquery_chunks_total").counter()
        assertThat(chunks?.count()).isEqualTo(2.0)
    }

    // ── metrics ───────────────────────────────────────────────────────────────

    @Test
    fun `bigquery_chunks_total increments once per chunk on normal batch`() {
        stubSuccess(websiteEventWriter, eventDataWriter, sessionWriter, sessionDataWriter)

        service.sendEvents(listOf(eventWithData(isNewSession = true)))

        val chunks = meterRegistry.find("bigquery_chunks_total").counter()
        assertThat(chunks?.count()).isEqualTo(4.0)
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private fun stubSuccess(vararg writers: JsonStreamWriter) {
        val okResponse = AppendRowsResponse.newBuilder()
            .setAppendResult(
                AppendRowsResponse.AppendResult.newBuilder()
                    .setOffset(Int64Value.of(0))
                    .build()
            )
            .build()
        writers.forEach { writer ->
            whenever(writer.append(any<JSONArray>())).thenReturn(ApiFutures.immediateFuture(okResponse))
        }
    }

    private fun eventWithData(isNewSession: Boolean) = EnrichedEvent(
        websiteId = UUID.randomUUID(),
        sessionId = UUID.randomUUID().toString(),
        createdAt = Instant.now(),
        isNewSession = isNewSession,
        websiteEvent = WebsiteEvent(
            eventId = UUID.randomUUID(),
            eventType = 2,
            eventName = "click",
            urlPath = "/test",
            hostname = "test.nav.no",
            visitId = UUID.randomUUID().toString(),
            eventData = listOf(
                EventDataEntry(eventDataId = UUID.randomUUID(), dataKey = "key", dataType = 1, stringValue = "val")
            )
        ),
        session = Session(
            browser = "Chrome", os = "macOS", device = "desktop",
            screen = "1920x1080", language = "nb", country = "NO",
            distinctId = "user-1",
            sessionData = listOf(
                SessionDataEntry(
                    sessionDataId = UUID.randomUUID(), sessionId = "s1",
                    dataKey = "role", dataType = 1, stringValue = "admin", distinctId = "user-1"
                )
            )
        )
    )

    private fun eventWithoutData(isNewSession: Boolean) = EnrichedEvent(
        websiteId = UUID.randomUUID(),
        sessionId = UUID.randomUUID().toString(),
        createdAt = Instant.now(),
        isNewSession = isNewSession,
        websiteEvent = WebsiteEvent(
            eventId = UUID.randomUUID(),
            eventType = 1,
            urlPath = "/page",
            hostname = "test.nav.no",
            visitId = UUID.randomUUID().toString(),
        ),
        session = Session(browser = "Chrome", os = "macOS", device = "desktop",
            screen = "1920x1080", language = "nb", country = "NO")
    )

    private fun websiteChangeEvent() = WebsiteChangeEvent(
        website = Website(
            websiteId = UUID.randomUUID(),
            name = "Test",
            domain = "test.nav.no",
            updatedAt = Instant.now()
        ),
        status = WebsiteEventStatus.CREATE
    )
}
