package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests


    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(requestAverageProcessingTime)
        .build()

    private val timeout = requestAverageProcessingTime

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1000))

    private val semaphore = Semaphore(parallelRequests, true)
    private val maxRetryCount = 2

    fun handleDeadlinePassed(paymentId: UUID, transactionId: UUID) {
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Deadline passed")
        }
        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId")
        return
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
//        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
//        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = HttpRequest.newBuilder()
            .uri(URI("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .version(HttpClient.Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.noBody())
            .timeout(timeout)
            .build()

        var isAcquired = false

        fun makeRequest(retriesLeft: Int = maxRetryCount) {

            if (retriesLeft == 0) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId, reason: max retry count exceeded")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "max retry count")
                }
            }

            if (!isAcquired) {
                while (!semaphore.tryAcquire()) {
                    Thread.sleep(20)
                    if (now() >= deadline - requestAverageProcessingTime.toMillis()) {
                        return handleDeadlinePassed(paymentId, transactionId)
                    }
                }
            }
            isAcquired = true


            while (!rateLimiter.tick()) {
                Thread.sleep(20)
                if (now() >= deadline - requestAverageProcessingTime.toMillis()) {
                    semaphore.release()
                    return handleDeadlinePassed(paymentId, transactionId)
                }
            }

            try {
                client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAcceptAsync { response ->
                        val body = try {
                            mapper.readValue(response.body(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                        }

//                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                        if (!body.result) {
                            makeRequest(retriesLeft - 1)
                        } else {
                            paymentESService.update(paymentId) {
                                it.logProcessing(true, now(), transactionId, reason = body.message)
                            }
                        }
                    }.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        makeRequest(retriesLeft - 1)
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            } finally {
                semaphore.release()
            }
        }

        makeRequest()
    }


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()