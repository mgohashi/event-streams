DROP TABLE IF EXISTS `item`, `order` CASCADE;
CREATE TABLE IF NOT EXISTS `order` (id VARCHAR(36) PRIMARY KEY, placed_dt DATETIME NOT NULL, confirmed_dt DATETIME, delivered_dt DATETIME, cancelled_dt DATETIME);
CREATE TABLE IF NOT EXISTS `item` (id VARCHAR(36) PRIMARY KEY, order_id VARCHAR(36) NOT NULL, prod_id VARCHAR(20) NOT NULL, amount INT NOT NULL, `value` DECIMAL(10,2), FOREIGN KEY (order_id) REFERENCES `order`(id) ON DELETE CASCADE);
