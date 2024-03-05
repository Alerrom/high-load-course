package ru.quipy.payments.config

import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.ExternalServiceProperties
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong

class AccountRequestsInfo(
        private val properties: ExternalServiceProperties,
) {
    private val timestamps: MutableList<Long> = mutableListOf()
    private var indexOfLastRequest = 0
    private var pendingRequestsAmount = AtomicLong(0)

    fun addTimestamp() {
        timestamps.add(System.currentTimeMillis())
    }

    fun incrementPendingRequestsAmount() {
        pendingRequestsAmount.addAndGet(1)
    }

    fun decrementPendingRequestsAmount() {
        pendingRequestsAmount.addAndGet(-1)
    }

    fun getLastSecondRequestsAmount(): Int {
        val now = System.currentTimeMillis()
        val oneSecondAgo = now - 1000

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
