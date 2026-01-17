package io.github.evelynkk.orderplatform.inventory;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;

import java.util.Optional;

public interface InventoryItemRepository extends JpaRepository<InventoryItem, String> {

    /**
     * Takes a row lock for the duration of the transaction.
     *
     * <p>Pessimistic rather than optimistic on purpose. Stock is the one place in this platform
     * with genuinely hot keys — a promoted SKU draws every concurrent order at once — and under
     * that contention optimistic locking degenerates into a retry storm where most attempts are
     * wasted. Serialising on the row costs one lock wait and always makes progress.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<InventoryItem> findWithLockByProductId(String productId);
}
