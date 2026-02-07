CREATE TABLE accounts (
    customer_id   INTEGER PRIMARY KEY,
    first_name    VARCHAR(50) NOT NULL,
    last_name     VARCHAR(50) NOT NULL,
    address_1     VARCHAR(200) NOT NULL,
    address_2     VARCHAR(200),
    city          VARCHAR(100) NOT NULL,
    state         VARCHAR(50) NOT NULL,
    zip_code      VARCHAR(10) NOT NULL,
    join_date     DATE NOT NULL
);

CREATE INDEX idx_accounts_name ON accounts (last_name, first_name);
CREATE INDEX idx_accounts_state ON accounts (state);

CREATE TABLE products (
    product_id          INTEGER PRIMARY KEY,
    product_code        VARCHAR(10) NOT NULL UNIQUE,
    product_description VARCHAR(255) NOT NULL
);

CREATE INDEX idx_products_code ON products (product_code);

CREATE TABLE transactions (
    transaction_id      VARCHAR(50) PRIMARY KEY,
    transaction_date    DATE NOT NULL,
    product_id          INTEGER NOT NULL,
    product_code        VARCHAR(10) NOT NULL,
    product_description VARCHAR(255) NOT NULL,
    quantity            INTEGER NOT NULL,
    account_id          INTEGER NOT NULL,
    CONSTRAINT fk_transactions_account
        FOREIGN KEY (account_id) REFERENCES accounts (customer_id),
    CONSTRAINT fk_transactions_product
        FOREIGN KEY (product_id) REFERENCES products (product_id)
);

CREATE INDEX idx_transactions_date ON transactions (transaction_date);
CREATE INDEX idx_transactions_account ON transactions (account_id);
CREATE INDEX idx_transactions_product ON transactions (product_id);
