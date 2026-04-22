CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS dw.fact_sales CASCADE;
DROP TABLE IF EXISTS dw.dim_product CASCADE;
DROP TABLE IF EXISTS dw.dim_supplier CASCADE;
DROP TABLE IF EXISTS dw.dim_store CASCADE;
DROP TABLE IF EXISTS dw.dim_seller CASCADE;
DROP TABLE IF EXISTS dw.dim_customer CASCADE;
DROP TABLE IF EXISTS dw.dim_pet_category CASCADE;
DROP TABLE IF EXISTS dw.dim_product_category CASCADE;
DROP TABLE IF EXISTS dw.dim_date CASCADE;

CREATE TABLE dw.dim_date (
    date_key integer PRIMARY KEY,
    full_date date NOT NULL UNIQUE,
    year smallint NOT NULL,
    quarter smallint NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month smallint NOT NULL CHECK (month BETWEEN 1 AND 12),
    day smallint NOT NULL CHECK (day BETWEEN 1 AND 31),
    day_of_week smallint NOT NULL CHECK (day_of_week BETWEEN 1 AND 7)
);

CREATE TABLE dw.dim_product_category (
    product_category_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_name text NOT NULL UNIQUE
);

CREATE TABLE dw.dim_pet_category (
    pet_category_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_name text NOT NULL UNIQUE
);

CREATE TABLE dw.dim_customer (
    customer_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file text NOT NULL,
    source_customer_id integer NOT NULL,
    first_name text,
    last_name text,
    age integer CHECK (age IS NULL OR age >= 0),
    email text,
    country text,
    postal_code text,
    pet_type text,
    pet_name text,
    pet_breed text,
    UNIQUE (source_file, source_customer_id)
);

CREATE TABLE dw.dim_seller (
    seller_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file text NOT NULL,
    source_seller_id integer NOT NULL,
    first_name text,
    last_name text,
    email text,
    country text,
    postal_code text,
    UNIQUE (source_file, source_seller_id)
);

CREATE TABLE dw.dim_store (
    store_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_natural_key text NOT NULL UNIQUE,
    store_name text,
    location text,
    city text,
    state text,
    country text,
    phone text,
    email text
);

CREATE TABLE dw.dim_supplier (
    supplier_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    supplier_natural_key text NOT NULL UNIQUE,
    supplier_name text,
    contact_name text,
    email text,
    phone text,
    address text,
    city text,
    country text
);

CREATE TABLE dw.dim_product (
    product_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file text NOT NULL,
    source_product_id integer NOT NULL,
    product_name text,
    product_category_key integer NOT NULL REFERENCES dw.dim_product_category (product_category_key),
    pet_category_key integer NOT NULL REFERENCES dw.dim_pet_category (pet_category_key),
    supplier_key integer NOT NULL REFERENCES dw.dim_supplier (supplier_key),
    price numeric(12, 2) CHECK (price IS NULL OR price >= 0),
    stock_quantity integer CHECK (stock_quantity IS NULL OR stock_quantity >= 0),
    weight numeric(10, 2) CHECK (weight IS NULL OR weight >= 0),
    color text,
    size text,
    brand text,
    material text,
    description text,
    rating numeric(3, 1) CHECK (rating IS NULL OR rating BETWEEN 0 AND 5),
    reviews integer CHECK (reviews IS NULL OR reviews >= 0),
    release_date date,
    expiry_date date,
    UNIQUE (source_file, source_product_id)
);

CREATE TABLE dw.fact_sales (
    sale_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file text NOT NULL,
    source_sale_id integer NOT NULL,
    date_key integer NOT NULL REFERENCES dw.dim_date (date_key),
    customer_key integer NOT NULL REFERENCES dw.dim_customer (customer_key),
    seller_key integer NOT NULL REFERENCES dw.dim_seller (seller_key),
    product_key integer NOT NULL REFERENCES dw.dim_product (product_key),
    store_key integer NOT NULL REFERENCES dw.dim_store (store_key),
    sale_quantity integer NOT NULL CHECK (sale_quantity >= 0),
    unit_price numeric(12, 2) CHECK (unit_price IS NULL OR unit_price >= 0),
    sale_total_amount numeric(12, 2) NOT NULL CHECK (sale_total_amount >= 0),
    UNIQUE (source_file, source_sale_id)
);

CREATE INDEX idx_fact_sales_date_key ON dw.fact_sales (date_key);
CREATE INDEX idx_fact_sales_customer_key ON dw.fact_sales (customer_key);
CREATE INDEX idx_fact_sales_product_key ON dw.fact_sales (product_key);
CREATE INDEX idx_fact_sales_store_key ON dw.fact_sales (store_key);
