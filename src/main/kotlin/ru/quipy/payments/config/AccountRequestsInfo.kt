package ru.quipy.payments.config

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.orders.api.OrderPaymentStartedEvent
import ru.quipy.payments.logic.ExternalServiceProperties
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

class AccountRequestsInfo(
        private val properties: ExternalServiceProperties,
) {

    val logger: Logger = LoggerFactory.getLogger(AccountRequestsInfo::class.java)

    val mutex = ReentrantLock()

    private var timestamps: MutableList<Long> = arrayListOf()
    private var durations: MutableList<Long> = arrayListOf()

    private var queue: Queue<OrderPaymentStartedEvent> = LinkedBlockingQueue(2000)

    private var indexOfLastRequest = 0
    private var pendingRequestsAmount = AtomicLong(0)
    private var callCounter = 0
    private var durationCallCounter = 0

    fun getQueue(): Queue<OrderPaymentStartedEvent> {
        return queue;
    }
    fun addTimestamp() {
        timestamps.add(System.currentTimeMillis())
    }

    fun addDuration(duration: Long) {
        durations.add(duration)
    }

    fun getAverageDuration(): Long {
        ++durationCallCounter
//        logger.warn("avgDuration $durationCallCounter")

        if (durationCallCounter >= 1000) {
            durationCallCounter = 0
            durations = durations.subList(kotlin.math.max(durations.size - 10, 0), durations.size)
        }

        var sum: Long = 0
        var count = 0
        for (i in kotlin.math.max(0, durations.size - 10) until durations.size) {
            sum += durations[i]
            ++count
        }

        if (count == 0) return 0
//        logger.warn("HEHEHE ${sum.toFloat() / count.toFloat()}")
        return (sum.toFloat() / count.toFloat()).toLong()
    }

    fun incrementPendingRequestsAmount() {
        pendingRequestsAmount.addAndGet(1)
    }

    fun decrementPendingRequestsAmount() {
        pendingRequestsAmount.addAndGet(-1)
    }

    fun getLastSecondRequestsAmount(): Int {
        ++callCounter
        val now = System.currentTimeMillis()
        val oneSecondAgo = now - 1000

        if (callCounter >= 1000) {
            callCounter = 0
            timestamps = timestamps.subList(indexOfLastRequest, timestamps.size)
            indexOfLastRequest = 0
        }
        for (i in indexOfLastRequest until timestamps.size) {
            if (timestamps[i] > oneSecondAgo) {
                indexOfLastRequest = i

                return timestamps.size - i
            }
        }

        return 0
    }

    fun getPendingRequestsAmount(): Long {
        return pendingRequestsAmount.get()
    }

    fun getParallelRequests(): Int {
        return properties.parallelRequests
    }

    fun getRateLimitPerSec(): Int {
        return properties.rateLimitPerSec
    }

    fun getPriority(): Int {
        return properties.priority
    }

    fun getExternalServiceProperties(): ExternalServiceProperties {
        return properties
    }
}
