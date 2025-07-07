package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
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
    private val win = OngoingWindow(parallelRequests)

    private val queue: BlockingQueue<Runnable> = LinkedBlockingQueue()

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        queue,
        NamedThreadFactory("request-executor-${accountName}")
    )

    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val httpClientExecutor = Executors.newFixedThreadPool(16, NamedThreadFactory("http-client-${accountName}"))

    private val javaClient = HttpClient.newBuilder()
        .executor(httpClientExecutor)
        .version(HttpClient.Version.HTTP_2)
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        paymentExecutor.submit(PaymentTask(paymentId, accountName, deadline) {
            logger.warn("Queue size: ${queue.size}")
            logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms, dealine: $deadline") // todo sukhoa

            val transactionId = UUID.randomUUID()

            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            val request = HttpRequest.newBuilder()
                .uri(
                    URI(
                        "http://${paymentProviderHostPort}/external/process?serviceName=${serviceName}&token=${token}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=${requestAverageProcessingTime.multipliedBy(3)}"
                    )
                )
                .POST(BodyPublishers.noBody())
                .build()

            logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

            win.acquire() // порядок захвата важен может быть очередь на семафоре, что сломает rate limiter
            rateLimiter.tickBlocking()
            javaClient.sendAsync(request, BodyHandlers.ofString())
                .thenApply { response ->
                    val body = try {
                        mapper.readValue(response.body(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }
                    win.release()

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
                .exceptionally { e ->
                    win.release()
                    when (e) {
                        is SocketTimeoutException, is InterruptedIOException -> {
                            logger.error(
                                "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                                e
                            )
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }

                        else -> {
                            logger.error(
                                "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                                e
                            )

                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                }
        })
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}


data class PaymentTask(
    val paymentId: UUID,
    val accountName: String,
    val deadline: Long,
    val block: (PaymentTask) -> Unit
) : Runnable {
    override fun run() {
        try {
            block(this)
        } catch (e: Exception) {
            PaymentExternalSystemAdapterImpl.logger.error(
                "[$accountName] PaymentTask failed unexpectedly. PaymentId $paymentId",
                e
            )
        }
    }
}

public fun now() = System.currentTimeMillis()