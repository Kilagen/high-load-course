package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
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
        }, 0, 20000)
    }


    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    // (serviceName=onlineStore, accountName=acc-7, parallelRequests=50, rateLimitPerSec=8, price=30, averageProcessingTime=PT1.2S, enabled=false)
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient
        .Builder()
        .callTimeout(1300, TimeUnit.MILLISECONDS)
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

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        var attempt = 0
        var finished = false

        try {
            if (!semaphore.tryAcquire(deadline - now() - requestAverageProcessingTime.toMillis(), TimeUnit.MILLISECONDS)) {
                return handleDeadlinePassed(paymentId, transactionId)
            }
            rateLimiter.tickBlocking()

            while (!finished) {
                try {
                    val startCall = System.nanoTime()
                    client.newCall(request).execute().use { response ->
                        val duration = Duration.ofNanos(System.nanoTime() - startCall).toMillis()
                        responseTimes.add(duration)

                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        if (body.message?.contains("Temporary error") == true) {
                            attempt++
                            if (attempt >= maxRetryCount) {
                                finished = true
                            semaphore.release()
                            } else {
                                Thread.sleep(backoffMs)
                            }
                        } else {
                            finished = true
                            semaphore.release()
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                    }
                } catch (e: InterruptedIOException) {
                    attempt++
                    if (attempt >= maxRetryCount) {
                        finished = true
                        logger.error("[$accountName] maxRetryCount reached: $transactionId, payment: $paymentId error on attempt $attempt")
                            semaphore.release()
                    } else {
                        logger.error("[$accountName] timeout occurred: $transactionId, payment: $paymentId error on attempt $attempt")
                        Thread.sleep(backoffMs)
                    }
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()