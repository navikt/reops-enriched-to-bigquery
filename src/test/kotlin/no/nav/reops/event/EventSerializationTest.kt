package no.nav.reops.event

import tools.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

class EventSerializationTest {

    private val mapper = jacksonObjectMapper()

    @Test
    fun `enriched event without eventData serializes correctly`() {
        val event = sampleEnrichedEvent(eventData = null)

        val json = mapper.writeValueAsString(event)
        val node = mapper.readTree(json)

        assertNotNull(node["websiteEvent"])
        assertNotNull(node["session"])
    }

    @Test
    fun `enriched event with eventData serializes correctly`() {
        val event = sampleEnrichedEvent(
            eventData = listOf(
                EventDataEntry(eventDataId = UUID.randomUUID(), dataKey = "key1", dataType = 1, stringValue = "value1"),
                EventDataEntry(eventDataId = UUID.randomUUID(), dataKey = "key2", dataType = 2, numberValue = 42.0)
            )
        )

        val json = mapper.writeValueAsString(event)
        val node = mapper.readTree(json)

        val eventDataNode = node["websiteEvent"]["eventData"]
        assertNotNull(eventDataNode)
        assertEquals("value1", eventDataNode[0]["stringValue"].asString())
        assertEquals(42.0, eventDataNode[1]["numberValue"].asDouble())
    }

    @Test
    fun `enriched event round-trips through JSON`() {
        val event = sampleEnrichedEvent(
            eventData = listOf(
                EventDataEntry(eventDataId = UUID.randomUUID(), dataKey = "test", dataType = 1, stringValue = "hello")
            )
        )

        val json = mapper.writeValueAsString(event)
        val deserialized = mapper.readValue(json, EnrichedEvent::class.java)

        assertEquals(event.websiteId, deserialized.websiteId)
        assertEquals(event.websiteEvent.hostname, deserialized.websiteEvent.hostname)
        assertEquals(event.websiteEvent.eventData, deserialized.websiteEvent.eventData)
    }

    @Test
    fun `website without deletedAt omits field from JSON and deserializes with null`() {
        val event = WebsiteChangeEvent(
            website = Website(
                websiteId = UUID.fromString("1dbfe4e9-bf8b-45d9-9305-928f22200bc0"),
                name = "Active Website",
                domain = "active.nav.no",
                updatedAt = Instant.parse("2026-01-01T00:00:00Z")
            ),
            status = WebsiteEventStatus.CREATE
        )

        val json = mapper.writeValueAsString(event)
        val node = mapper.readTree(json)

        assertFalse(node["website"].has("deletedAt"))

        val deserialized = mapper.readValue(json, WebsiteChangeEvent::class.java)
        assertNull(deserialized.website.deletedAt)
        assertEquals(event.website.websiteId, deserialized.website.websiteId)
        assertEquals(event.status, deserialized.status)
    }

    @Test
    fun `website with deletedAt serializes and round-trips correctly`() {
        val deletedAt = Instant.parse("2026-01-02T12:00:00Z")
        val event = WebsiteChangeEvent(
            website = Website(
                websiteId = UUID.fromString("1dbfe4e9-bf8b-45d9-9305-928f22200bc0"),
                name = "Deleted Website",
                domain = "deleted.nav.no",
                updatedAt = Instant.parse("2026-01-01T00:00:00Z"),
                deletedAt = deletedAt
            ),
            status = WebsiteEventStatus.DELETE
        )

        val json = mapper.writeValueAsString(event)
        val node = mapper.readTree(json)

        assertTrue(node["website"].has("deletedAt"))
        assertEquals(deletedAt.toString(), node["website"]["deletedAt"].asString())

        val deserialized = mapper.readValue(json, WebsiteChangeEvent::class.java)
        assertEquals(deletedAt, deserialized.website.deletedAt)
        assertEquals(WebsiteEventStatus.DELETE, deserialized.status)
    }

    @Test
    fun `legacy website JSON without deletedAt deserializes with null deletedAt`() {
        val legacyJson = """
            {
              "website": {
                "websiteId": "1dbfe4e9-bf8b-45d9-9305-928f22200bc0",
                "name": "Legacy Website",
                "domain": "legacy.nav.no",
                "updatedAt": "2026-01-01T00:00:00Z",
                "teamId": "aaaaaaaa-1111-2222-3333-444444444444"
              },
              "status": "UPDATE"
            }
        """.trimIndent()

        val deserialized = mapper.readValue(legacyJson, WebsiteChangeEvent::class.java)

        assertNull(deserialized.website.deletedAt)
        assertEquals(UUID.fromString("aaaaaaaa-1111-2222-3333-444444444444"), deserialized.website.teamId)
        assertEquals(WebsiteEventStatus.UPDATE, deserialized.status)
    }

    private fun sampleEnrichedEvent(eventData: List<EventDataEntry>? = null): EnrichedEvent {
        val websiteId = UUID.fromString("1dbfe4e9-bf8b-45d9-9305-928f22200bc0")
        return EnrichedEvent(
            websiteId = websiteId,
            sessionId = UUID.randomUUID().toString(),
            createdAt = Instant.parse("2026-01-01T00:00:00Z"),
            websiteEvent = WebsiteEvent(
                eventId = UUID.randomUUID(),
                eventName = null,
                eventType = 1,
                urlPath = "/test",
                pageTitle = "Test Page",
                referrerPath = null,
                referrerDomain = null,
                visitId = UUID.randomUUID().toString(),
                hostname = "localhost",
                eventData = eventData
            ),
            session = Session(
                browser = "Chrome",
                os = "macOS",
                device = "desktop",
                screen = "1920x1080",
                language = "en",
                country = "NO"
            )
        )
    }
}
