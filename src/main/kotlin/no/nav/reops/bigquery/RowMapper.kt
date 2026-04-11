package no.nav.reops.bigquery

import no.nav.reops.event.EnrichedEvent
import no.nav.reops.event.WebsiteChangeEvent
import org.json.JSONObject
import java.time.Instant
import java.util.concurrent.TimeUnit

/**
 * Maps [EnrichedEvent] to flat [JSONObject] rows for each BigQuery table.
 *
 * The column names use snake_case to match the BigQuery table schemas.
 * TIMESTAMP columns use microseconds since epoch as required by the
 * BigQuery Storage Write API's JsonStreamWriter.
 */
object RowMapper {

    private fun Instant.toMicros(): Long =
        TimeUnit.SECONDS.toMicros(epochSecond) + TimeUnit.NANOSECONDS.toMicros(nano.toLong())

    fun toWebsiteEventRow(event: EnrichedEvent): JSONObject {
        val we = event.websiteEvent
        return JSONObject().apply {
            put("event_id", we.eventId.toString())
            put("website_id", event.websiteId.toString())
            put("session_id", event.sessionId)
            put("created_at", event.createdAt.toMicros())
            put("url_path", we.urlPath)
            put("url_query", we.urlQuery)
            put("referrer_path", we.referrerPath)
            put("referrer_query", we.referrerQuery)
            put("referrer_domain", we.referrerDomain)
            put("page_title", we.pageTitle)
            put("event_type", we.eventType)
            put("event_name", we.eventName)
            put("visit_id", we.visitId.toString())
            put("hostname", we.hostname)
        }
    }

    fun toEventDataRows(event: EnrichedEvent): List<JSONObject> {
        val entries = event.websiteEvent.eventData ?: return emptyList()
        return entries.map { entry ->
            JSONObject().apply {
                put("event_data_id", entry.eventDataId.toString())
                put("website_id", event.websiteId.toString())
                put("website_event_id", event.websiteEvent.eventId.toString())
                put("data_key", entry.dataKey)
                put("string_value", entry.stringValue)
                put("number_value", entry.numberValue)
                entry.dateValue?.let { put("date_value", it.toMicros()) }
                put("data_type", entry.dataType)
                put("created_at", event.createdAt.toMicros())
            }
        }
    }

    fun toSessionRow(event: EnrichedEvent): JSONObject {
        val s = event.session
        return JSONObject().apply {
            put("session_id", event.sessionId)
            put("website_id", event.websiteId.toString())
            put("browser", s.browser)
            put("os", s.os)
            put("device", s.device)
            put("screen", s.screen)
            put("language", s.language)
            put("country", s.country)
            put("created_at", event.createdAt.toMicros())
            put("distinct_id", s.distinctId)
            put("hostname", event.websiteEvent.hostname)
        }
    }

    fun toSessionDataRows(event: EnrichedEvent): List<JSONObject> {
        val entries = event.session.sessionData ?: return emptyList()
        return entries.map { entry ->
            JSONObject().apply {
                put("session_data_id", entry.sessionDataId.toString())
                put("website_id", event.websiteId.toString())
                put("session_id", entry.sessionId)
                put("data_key", entry.dataKey)
                put("string_value", entry.stringValue)
                put("number_value", entry.numberValue)
                entry.dateValue?.let { put("date_value", it.toMicros()) }
                put("data_type", entry.dataType)
                put("created_at", event.createdAt.toMicros())
                put("distinct_id", entry.distinctId)
            }
        }
    }

    fun toWebsiteRow(event: WebsiteChangeEvent): JSONObject {
        val w = event.website
        return JSONObject().apply {
            put("website_id", w.websiteId.toString())
            put("name", w.name)
            put("domain", w.domain)
            put("updated_at", w.updatedAt.toMicros())
        }
    }
}
