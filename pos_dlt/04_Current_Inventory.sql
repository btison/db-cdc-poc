-- Databricks notebook source
-- DBTITLE 1,Define Current Inventory Table (SQL)
SET pipelines.trigger.interval = 5 minute;

CREATE LIVE TABLE inventory_current 
COMMENT 'calculate current inventory given the latest inventory snapshots and inventory-relevant events' 
TBLPROPERTIES (
  'quality'='gold'
  ) 
AS
  SELECT  -- calculate current inventory
    a.store_id,
    a.item_id,
    FIRST(a.quantity) as snapshot_quantity,
    COALESCE(SUM(b.quantity), 0) as change_quantity,
    FIRST(a.quantity) + COALESCE(SUM(b.quantity), 0) as current_inventory,
    GREATEST(FIRST(a.date_time_ts), MAX(b.date_time)) as date_time
  FROM LIVE.inventory_snapshot a -- access latest snapshot
  LEFT OUTER JOIN ( -- calculate inventory change with bopis corrections
    SELECT
      x.store_id,
      x.item_id,
      x.date_time,
      x.quantity
    FROM LIVE.inventory_change x
      INNER JOIN LIVE.store y ON x.store_id = y.store_id
      INNER JOIN LIVE.inventory_change_type z ON x.change_type_id = z.change_type_id
    WHERE NOT( y.name = 'online' AND z.change_type = 'bopis') -- exclude bopis records from online store
    ) b 
    ON  
      a.store_id = b.store_id AND
      a.item_id = b.item_id AND
      a.date_time_ts <= b.date_time
  GROUP BY
    a.store_id,
    a.item_id
  ORDER BY 
    date_time DESC
