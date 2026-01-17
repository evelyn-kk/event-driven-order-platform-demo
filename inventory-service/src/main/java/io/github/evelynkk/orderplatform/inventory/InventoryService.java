package io.github.evelynkk.orderplatform.inventory;

import io.github.evelynkk.orderplatform.events.DomainEvent;
import io.github.evelynkk.orderplatform.events.InventoryDeductedEvent;
import io.github.evelynkk.orderplatform.events.InventoryInsufficientEvent;
import io.github.evelynkk.orderplatform.events.OrderCreatedEvent;
import io.github.evelynkk.orderplatform.messaging.outbox.OutboxRecorder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService {

    private static final String AGGREGATE_TYPE = "inventory";

    private final InventoryItemRepository items;
    private final StockReservationRepository reservations;
    private final OutboxRecorder outbox;

    /**
     * Holds stock for an order, or reports that it cannot.
     *
     * <p>Both outcomes are equally valid saga results, so neither throws: the outcome is announced
     * as an event and the order aggregate decides what it means.
     */
    @Transactional
    public void reserve(OrderCreatedEvent event) {
        Optional<InventoryItem> found = items.findWithLockByProductId(event.productId());

        // An unknown product has no supply. Reporting zero available is more honest than inventing
        // stock, and it keeps the saga on a path the order aggregate already handles.
        if (found.isEmpty() || !found.get().reserve(event.quantity())) {
            int available = found.map(InventoryItem::getAvailable).orElse(0);
            log.warn("Cannot reserve {} of {} for order {}: {} available",
                    event.quantity(), event.productId(), event.orderId(), available);

            outbox.record(AGGREGATE_TYPE, "inventory.insufficient", new InventoryInsufficientEvent(
                    DomainEvent.newEventId(),
                    event.orderId(),
                    event.productId(),
                    event.quantity(),
                    available,
                    Instant.now()));
            return;
        }

        InventoryItem item = found.get();
        reservations.save(new StockReservation(event.orderId(), event.productId(), event.quantity()));

        outbox.record(AGGREGATE_TYPE, "inventory.deducted", new InventoryDeductedEvent(
                DomainEvent.newEventId(),
                event.orderId(),
                event.productId(),
                event.quantity(),
                item.getAvailable(),
                Instant.now()));

        log.info("Reserved {} of {} for order {}; {} available",
                event.quantity(), event.productId(), event.orderId(), item.getAvailable());
    }

    /**
     * Compensating action for a cancelled order: give the held stock back.
     *
     * <p>Cancellation also fires for orders that never got a reservation — an order rejected for
     * insufficient stock is cancelled too — so a missing reservation is expected, not an error.
     */
    @Transactional
    public void release(String orderId) {
        StockReservation reservation = reservations.findById(orderId).orElse(null);
        if (reservation == null) {
            log.debug("Order {} cancelled with no reservation to release", orderId);
            return;
        }
        if (!reservation.release()) {
            log.debug("Reservation for order {} already {}", orderId, reservation.getState());
            return;
        }

        InventoryItem item = items.findWithLockByProductId(reservation.getProductId()).orElseThrow();
        item.release(reservation.getQuantity());

        log.info("Released {} of {} from cancelled order {}; {} available",
                reservation.getQuantity(), reservation.getProductId(), orderId, item.getAvailable());
    }

    /** The goods shipped, so the hold is consumed rather than returned to available stock. */
    @Transactional
    public void commit(String orderId) {
        StockReservation reservation = reservations.findById(orderId).orElse(null);
        if (reservation == null || !reservation.commit()) {
            return;
        }

        InventoryItem item = items.findWithLockByProductId(reservation.getProductId()).orElseThrow();
        item.commit(reservation.getQuantity());

        log.info("Committed {} of {} for shipped order {}",
                reservation.getQuantity(), reservation.getProductId(), orderId);
    }
}
