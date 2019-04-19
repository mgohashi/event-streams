DROP TABLE IF EXISTS `product` CASCADE;
CREATE TABLE IF NOT EXISTS `product` (id VARCHAR(36) PRIMARY KEY, `name` VARCHAR(300) NOT NULL, amount INT NOT NULL DEFAULT 0, unit VARCHAR(10));
INSERT into `product` VALUES(1, 'Café', 1000, 'ml'), (2, 'Leite', 1000, 'ml'),(3, 'Pao na Chapa', 10, null);