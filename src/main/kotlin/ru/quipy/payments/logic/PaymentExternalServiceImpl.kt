package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.timerTask


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    private val timer = Timer()
    private val responseTimes = Collections.synchronizedList(mutableListOf<Long>())

    init {
        timer.scheduleAtFixedRate(timerTask {
            val responseTimesString = responseTimes.joinToString(separator = ", ") { it.toString() }
            logger.info("responseTimes [$responseTimesString]")
        }, 10000, 20000)
    }


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

    private val client = OkHttpClient
        .Builder()
        .callTimeout(10000, TimeUnit.MILLISECONDS)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1020))

    private val semaphore = Semaphore(parallelRequests, true)
    private val maxRetryCount = 4
    private val backoffMs = 100L

    fun handleDeadlinePassed(paymentId: UUID, transactionId: UUID) {
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Deadline passed")
        }
        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId")
        return
    }
    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        val startTime = now()
        val timeoutMs = deadline - startTime - requestAverageProcessingTime.toMillis()

        if (!semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
            handleDeadlinePassed(paymentId, transactionId)
            return
        }

        rateLimiter.tickBlocking()

        var attempt = 0

        fun makeRequest() {
            client.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    attempt++
                    if (attempt >= maxRetryCount || now() > deadline) {
                        logger.error("[$accountName] maxRetryCount reached: $transactionId, payment: $paymentId error on attempt $attempt")
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                        semaphore.release()
                    } else {
                        logger.warn("[$accountName] timeout occurred: $transactionId, payment: $paymentId error on attempt $attempt")
                        rateLimiter.tickBlocking()
                        makeRequest()
                    }
                }

                override fun onResponse(call: Call, response: Response) {
                    response.use {
                        val duration = Duration.ofNanos(System.nanoTime()).toMillis()
                        responseTimes.add(duration)

                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        if (body.message?.contains("Temporary error") == true && attempt < maxRetryCount) {
                            attempt++
                            rateLimiter.tickBlocking()
                            makeRequest()
                            return
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                        semaphore.release()
                    }
                }
            })
        }

        makeRequest()
    }


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()