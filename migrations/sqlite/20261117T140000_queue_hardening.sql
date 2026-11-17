CREATE UNIQUE INDEX queue_items_one_active
    ON queue_items (device_id)
    WHERE state IN ('starting', 'printing');
