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
import ru.quipy.payments.logic.*
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.*
import java.util.concurrent.Executors
import javax.annotation.PostConstruct
import java.util.concurrent.locks.ReentrantLock

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

    private val paymentExecutor = Executors.newFixedThreadPool(16, NamedThreadFactory("payment-executor"))

    private val accounts: List<AccountRequestsInfo> = mutableListOf(
            AccountRequestsInfo(ExternalServicesConfig.accountProps_1),
            AccountRequestsInfo(ExternalServicesConfig.accountProps_2))

    private val mutex = ReentrantLock()

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(OrderAggregate::class, "payments:order-subscriber", retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)) {
            `when`(OrderPaymentStartedEvent::class) { event ->
                paymentExecutor.submit {
                    val createdEvent = paymentESService.create {
                        it.create(
                                event.paymentId,
                                event.orderId,
                                event.amount
                        )
                    }
                    logger.warn("Payment ${createdEvent.paymentId} for order ${event.orderId} created.")

                    mutex.lock()
                    val accountInfo = accounts.maxByOrNull { if (it.getPendingRequestsAmount() <= it.getParallelRequests() && it.getLastSecondRequestsAmount() <= it.getRateLimitPerSec()) it.getPriority() else 0 }
                    logger.warn("HEHE ${accountInfo?.getExternalServiceProperties()?.accountName}")

                    accountInfo?.addTimestamp()
                    accountInfo?.incrementPendingRequestsAmount()
                    mutex.unlock()
                    val paymentService = PaymentExternalServiceImpl(accountInfo!!, mutex)

                    paymentService.submitPaymentRequest(createdEvent.paymentId, event.amount, event.createdAt)
                }
            }
        }
    }
}