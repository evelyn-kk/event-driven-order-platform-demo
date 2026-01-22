package io.github.evelynkk.orderplatform.shipping;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

/**
 * Keyed by order rather than by shipment id, which is what makes creation idempotent at the
 * storage layer: a redelivered {@code payment.completed} collides on the primary key instead of
 * booking a second courier pickup.
 */
@Entity
@Table(name = "shipment")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Shipment {

    private static final String CARRIER = "SF-Express";

    @Id
    @Column(name = "order_id", nullable = false, updatable = false, length = 64)
    private String orderId;

    @Column(name = "shipment_id", nullable = false, length = 64)
    private String shipmentId;

    @Column(name = "carrier", nullable = false, length = 64)
    private String carrier;

    @Column(name = "tracking_number", nullable = false, length = 64)
    private String trackingNumber;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    public Shipment(String orderId) {
        this.orderId = orderId;
        this.shipmentId = UUID.randomUUID().toString();
        this.carrier = CARRIER;
        this.trackingNumber = "SF" + UUID.randomUUID().toString()
                .substring(0, 10).toUpperCase(Locale.ROOT);
        this.createdAt = Instant.now();
    }
}
