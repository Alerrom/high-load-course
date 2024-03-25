package ru.quipy.payments.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.api.OrderPaymentStartedEvent
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.AccountRequestsInfo
import ru.quipy.payments.config.ExternalServicesConfig
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import ru.quipy.payments.logic.create
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.PostConstruct
import kotlin.math.min

@Service
class OrderPaymentSubscriber {

    val logger: Logger = LoggerFactory.getLogger(OrderPaymentSubscriber::class.java)

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

//    @Autowired
//    @Qualifier(ExternalServicesConfig.PRIMARY_PAYMENT_BEAN)
//    private lateinit var paymentService: PaymentService

    private val paymentExecutor = Executors.newFixedThreadPool(300, NamedThreadFactory("payment-executor"))
    private val realPaymentExecutor = Executors.newFixedThreadPool(1000, NamedThreadFactory("real-payment-executor"))

    private val accounts: List<AccountRequestsInfo> = mutableListOf(
        AccountRequestsInfo(ExternalServicesConfig.accountProps_1),
        AccountRequestsInfo(ExternalServicesConfig.accountProps_2)
    )

    private val queueProcessor = Executors.newFixedThreadPool(accounts.size, NamedThreadFactory("queue-processor"))

    private val mutex = ReentrantLock()
    private var stop: Boolean = false
    fun processQueue(accountInfo: AccountRequestsInfo) {
        logger.warn("START NEW QUEUE ${accountInfo.getExternalServiceProperties().accountName}")
        val paymentService = PaymentExternalServiceImpl(accountInfo, mutex, paymentESService)
        while (!stop) {
            logger.warn("NEW ITER ${accountInfo.getExternalServiceProperties().accountName}")
            accountInfo.mutex.lock()
            if (accountInfo.getQueue().size == 0) {
                accountInfo.mutex.unlock()
                Thread.sleep(75)
                continue
            }

            val currTime = System.currentTimeMillis()
            val event = accountInfo.getQueue().peek()!!

            logger.warn("[HEHE 1] NEW ACC IN QUEUE ${event.orderId}")

            if (currTime + accountInfo.getAverageDuration() - event.createdAt >= 80_000) {
                logger.warn("NOT HEHE ${currTime + accountInfo.getAverageDuration() - event.createdAt >= 80_000}")
                accountInfo.getQueue().remove()
            }

            if (accountInfo.getPendingRequestsAmount() < accountInfo.getParallelRequests() && accountInfo.getLastSecondRequestsAmount() < accountInfo.getRateLimitPerSec()) {
                accountInfo.addTimestamp()
                accountInfo.incrementPendingRequestsAmount()
                accountInfo.getQueue().remove()
                accountInfo.mutex.unlock()


                realPaymentExecutor.submit {
                    paymentService.submitPaymentRequest(
                        event.paymentId,
                        event.amount,
                        event.createdAt
                    )
                }

            } else {
                accountInfo.mutex.unlock()
            }

            Thread.sleep(75)
            continue
        }
    }

    fun getPriority(accountInfo: AccountRequestsInfo, event: OrderPaymentStartedEvent): Int {

        val currTime = System.currentTimeMillis()

        accountInfo.mutex.lock()
        if (accountInfo.getPendingRequestsAmount() < accountInfo.getParallelRequests() && accountInfo.getLastSecondRequestsAmount() < accountInfo.getRateLimitPerSec() && (currTime + accountInfo.getAverageDuration() - event.createdAt) < 80_000) {
            accountInfo.mutex.unlock()
            return accountInfo.getPriority()
        } else {
            accountInfo.mutex.unlock()
            return 0
        }
    }

    @PostConstruct
    fun init() {
        for (acc in accounts) {
            queueProcessor.submit { processQueue(acc) }
        }

        subscriptionsManager.createSubscriber(
            OrderAggregate::class,
            "payments:order-subscriber",
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {
            `when`(OrderPaymentStartedEvent::class) { event ->
                paymentExecutor.submit {
                    val createdEvent = paymentESService.create {
                        it.create(
                            event.paymentId,
                            event.orderId,
                            event.amount,
                        )
                    }
                    logger.warn("Payment ${createdEvent.paymentId} for order ${event.orderId} created.")


                    val accountInfo =
                        accounts.maxByOrNull { getPriority(it, event) }!!

                    logger.warn("SELECT ACC ${accountInfo.getExternalServiceProperties().accountName}")

                    accountInfo.mutex.lock()
                    accountInfo.getQueue().add(event)
                    accountInfo.mutex.unlock()


                    logger.warn("[HEHE 1] PendingRequestsAmount: ${accountInfo.getPendingRequestsAmount()}, ParallelRequests: ${accountInfo.getParallelRequests()}")
                    logger.warn("[HEHE 2] LastSecondRequestsAmount: ${accountInfo.getLastSecondRequestsAmount()}, RateLimitPerSec: ${accountInfo.getRateLimitPerSec()}")
//
//                    logger.warn("[HEHE 3] AccountName: ${accountInfo.getExternalServiceProperties().accountName}, AccountPriority: ${accountInfo.getExternalServiceProperties().priority}")

                    logger.warn(
                        "[THROUGHPUT] ACC: ${accountInfo.getExternalServiceProperties().accountName} THROUGHPUT: ${
                            min(
                                accountInfo.getExternalServiceProperties().parallelRequests.toFloat() / (accountInfo.getAverageDuration() / 1000),
                                accountInfo.getExternalServiceProperties().rateLimitPerSec.toFloat()
                            )
                        }"
                    )
                }
            }
        }
    }
}