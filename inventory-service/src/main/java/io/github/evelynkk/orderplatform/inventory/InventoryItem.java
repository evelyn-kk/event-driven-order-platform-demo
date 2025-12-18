package io.github.evelynkk.orderplatform.inventory;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Stock for one product, split into what can still be sold and what is spoken for.
 *
 * <p>Keeping {@code reserved} rather than just decrementing {@code available} means the two halves
 * of the saga are auditable: at any moment reserved stock equals the sum of open reservations, so
 * a leak — a compensation that never ran — is visible rather than silently absorbed.
 */
@Entity
@Table(name = "inventory_item")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class InventoryItem {

    @Id
    @Column(name = "product_id", nullable = false, updatable = false, length = 64)
    private String productId;

    @Column(name = "available", nullable = false)
    private int available;

    @Column(name = "reserved", nullable = false)
    private int reserved;

    public InventoryItem(String productId, int available) {
        this.productId = productId;
        this.available = available;
        this.reserved = 0;
    }

    /** @return false if there is not enough stock, leaving the item untouched */
    public boolean reserve(int quantity) {
        if (quantity > available) {
            return false;
        }
        available -= quantity;
        reserved += quantity;
        return true;
    }

    /** Compensation: the order failed downstream, so the stock becomes sellable again. */
    public void release(int quantity) {
        reserved -= quantity;
        available += quantity;
    }

    /** The goods shipped, so the reservation is consumed rather than returned. */
    public void commit(int quantity) {
        reserved -= quantity;
    }
}
