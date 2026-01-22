package io.github.evelynkk.orderplatform.shipping;

import io.github.evelynkk.orderplatform.events.DomainEvent;
import io.github.evelynkk.orderplatform.events.ShippingCreatedEvent;
import io.github.evelynkk.orderplatform.messaging.outbox.OutboxRecorder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShippingService {

    private static final String AGGREGATE_TYPE = "shipment";

    private final ShipmentRepository shipments;
    private final OutboxRecorder outbox;

    @Transactional
    public void createShipment(String orderId) {
        if (shipments.existsById(orderId)) {
            log.debug("Shipment already exists for order {}", orderId);
            return;
        }

        Shipment shipment = shipments.save(new Shipment(orderId));

        outbox.record(AGGREGATE_TYPE, "shipping.created", new ShippingCreatedEvent(
                DomainEvent.newEventId(),
                orderId,
                shipment.getShipmentId(),
                shipment.getCarrier(),
                shipment.getTrackingNumber(),
                Instant.now()));

        log.info("Created shipment {} for order {}, tracking {}",
                shipment.getShipmentId(), orderId, shipment.getTrackingNumber());
    }
}
