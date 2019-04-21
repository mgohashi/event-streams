DROP TABLE IF EXISTS `product` CASCADE;
CREATE TABLE IF NOT EXISTS `product` (id VARCHAR(36) PRIMARY KEY, `name` VARCHAR(300) NOT NULL, amount INT NOT NULL DEFAULT 0, unit VARCHAR(10));
INSERT into `product` VALUES(1, 'Coffee', 1000, 'g'), (2, 'Milk', 1000, 'ml'),(3, 'Grilled bread', 10, 'un');