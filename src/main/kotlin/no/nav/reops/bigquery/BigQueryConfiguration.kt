package no.nav.reops.bigquery

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings
import io.grpc.ManagedChannelBuilder
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.io.ByteArrayInputStream

@Configuration
class BigQueryConfiguration {

    @Bean
    @Profile("!local")
    fun bigQueryWriteClient(): BigQueryWriteClient {
        val credentialsJson = System.getenv("bigquery-credentials")
        if (!credentialsJson.isNullOrBlank()) {
            LOG.info("Using credentials from bigquery-credentials secret (NAIS)")
            val credentials = ServiceAccountCredentials.fromStream(
                ByteArrayInputStream(credentialsJson.toByteArray())
            )
            val settings = BigQueryWriteSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .build()
            return BigQueryWriteClient.create(settings)
        }
        LOG.info("Using application default credentials")
        return BigQueryWriteClient.create()
    }

    @Bean
    @Profile("local")
    fun localBigQueryWriteClient(
        @Value("\${reops.bigquery.endpoint}") endpoint: String
    ): BigQueryWriteClient {
        val channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build()
        val settings = BigQueryWriteSettings.newBuilder()
            .setTransportChannelProvider(FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)))
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build()
        return BigQueryWriteClient.create(settings)
    }

    private companion object {
        private val LOG = LoggerFactory.getLogger(BigQueryConfiguration::class.java)
    }
}
