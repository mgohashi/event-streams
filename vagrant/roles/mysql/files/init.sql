CREATE USER 'user'@'%' IDENTIFIED BY 'test123';
GRANT ALL PRIVILEGES ON * . * TO 'user'@'%';
CREATE DATABASE coffee_store;
CREATE DATABASE coffee_stock;
