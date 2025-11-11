-- SQL script to initialize database for wallet asset management
CREATE TABLE assets (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    value DECIMAL(10, 2) NOT NULL
);

INSERT INTO assets (id, name, value) VALUES
(1, 'Bitcoin', 20000.00),
(2, 'Ethereum', 1500.00);