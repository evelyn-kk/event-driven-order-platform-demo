package io.github.evelynkk.orderplatform.inventory;

import org.springframework.data.jpa.repository.JpaRepository;

public interface StockReservationRepository extends JpaRepository<StockReservation, String> {
}
