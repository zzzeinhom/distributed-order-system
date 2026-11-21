package com.zzzeinhom.inventory.repository;

import com.zzzeinhom.inventory.entity.ProductInventory;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface ProductInventoryRepository extends JpaRepository<ProductInventory, UUID> {}
