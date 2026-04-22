TRUNCATE TABLE
    dw.fact_sales,
    dw.dim_product,
    dw.dim_supplier,
    dw.dim_store,
    dw.dim_seller,
    dw.dim_customer,
    dw.dim_pet_category,
    dw.dim_product_category,
    dw.dim_date
RESTART IDENTITY CASCADE;

INSERT INTO dw.dim_date (date_key, full_date, year, quarter, month, day, day_of_week)
SELECT DISTINCT
    to_char(sale_dt, 'YYYYMMDD')::integer AS date_key,
    sale_dt AS full_date,
    extract(year FROM sale_dt)::smallint AS year,
    extract(quarter FROM sale_dt)::smallint AS quarter,
    extract(month FROM sale_dt)::smallint AS month,
    extract(day FROM sale_dt)::smallint AS day,
    extract(isodow FROM sale_dt)::smallint AS day_of_week
FROM (
    SELECT to_date(sale_date, 'MM/DD/YYYY') AS sale_dt
    FROM raw.mock_data
    WHERE NULLIF(sale_date, '') IS NOT NULL
) AS dates;

INSERT INTO dw.dim_product_category (category_name)
SELECT DISTINCT product_category
FROM raw.mock_data
WHERE NULLIF(product_category, '') IS NOT NULL
ORDER BY product_category;

INSERT INTO dw.dim_pet_category (category_name)
SELECT DISTINCT pet_category
FROM raw.mock_data
WHERE NULLIF(pet_category, '') IS NOT NULL
ORDER BY pet_category;

INSERT INTO dw.dim_customer (
    source_file,
    source_customer_id,
    first_name,
    last_name,
    age,
    email,
    country,
    postal_code,
    pet_type,
    pet_name,
    pet_breed
)
SELECT DISTINCT ON (source_file, sale_customer_id::integer)
    source_file,
    sale_customer_id::integer,
    customer_first_name,
    customer_last_name,
    customer_age::integer,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM raw.mock_data
WHERE NULLIF(sale_customer_id, '') IS NOT NULL
ORDER BY source_file, sale_customer_id::integer, raw_row_id;

INSERT INTO dw.dim_seller (
    source_file,
    source_seller_id,
    first_name,
    last_name,
    email,
    country,
    postal_code
)
SELECT DISTINCT ON (source_file, sale_seller_id::integer)
    source_file,
    sale_seller_id::integer,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM raw.mock_data
WHERE NULLIF(sale_seller_id, '') IS NOT NULL
ORDER BY source_file, sale_seller_id::integer, raw_row_id;

INSERT INTO dw.dim_store (
    store_natural_key,
    store_name,
    location,
    city,
    state,
    country,
    phone,
    email
)
SELECT DISTINCT ON (store_natural_key)
    store_natural_key,
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM (
    SELECT
        md5(concat_ws('|',
            coalesce(store_name, ''),
            coalesce(store_location, ''),
            coalesce(store_city, ''),
            coalesce(store_state, ''),
            coalesce(store_country, ''),
            coalesce(store_phone, ''),
            coalesce(store_email, '')
        )) AS store_natural_key,
        store_name,
        store_location,
        store_city,
        store_state,
        store_country,
        store_phone,
        store_email,
        raw_row_id
    FROM raw.mock_data
) AS stores
ORDER BY store_natural_key, raw_row_id;

INSERT INTO dw.dim_supplier (
    supplier_natural_key,
    supplier_name,
    contact_name,
    email,
    phone,
    address,
    city,
    country
)
SELECT DISTINCT ON (supplier_natural_key)
    supplier_natural_key,
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM (
    SELECT
        md5(concat_ws('|',
            coalesce(supplier_name, ''),
            coalesce(supplier_contact, ''),
            coalesce(supplier_email, ''),
            coalesce(supplier_phone, ''),
            coalesce(supplier_address, ''),
            coalesce(supplier_city, ''),
            coalesce(supplier_country, '')
        )) AS supplier_natural_key,
        supplier_name,
        supplier_contact,
        supplier_email,
        supplier_phone,
        supplier_address,
        supplier_city,
        supplier_country,
        raw_row_id
    FROM raw.mock_data
) AS suppliers
ORDER BY supplier_natural_key, raw_row_id;

INSERT INTO dw.dim_product (
    source_file,
    source_product_id,
    product_name,
    product_category_key,
    pet_category_key,
    supplier_key,
    price,
    stock_quantity,
    weight,
    color,
    size,
    brand,
    material,
    description,
    rating,
    reviews,
    release_date,
    expiry_date
)
SELECT DISTINCT ON (r.source_file, r.sale_product_id::integer)
    r.source_file,
    r.sale_product_id::integer,
    r.product_name,
    pc.product_category_key,
    pet.pet_category_key,
    s.supplier_key,
    r.product_price::numeric(12, 2),
    r.product_quantity::integer,
    r.product_weight::numeric(10, 2),
    r.product_color,
    r.product_size,
    r.product_brand,
    r.product_material,
    r.product_description,
    r.product_rating::numeric(3, 1),
    r.product_reviews::integer,
    to_date(r.product_release_date, 'MM/DD/YYYY'),
    to_date(r.product_expiry_date, 'MM/DD/YYYY')
FROM raw.mock_data r
JOIN dw.dim_product_category pc
    ON pc.category_name = r.product_category
JOIN dw.dim_pet_category pet
    ON pet.category_name = r.pet_category
JOIN dw.dim_supplier s
    ON s.supplier_natural_key = md5(concat_ws('|',
        coalesce(r.supplier_name, ''),
        coalesce(r.supplier_contact, ''),
        coalesce(r.supplier_email, ''),
        coalesce(r.supplier_phone, ''),
        coalesce(r.supplier_address, ''),
        coalesce(r.supplier_city, ''),
        coalesce(r.supplier_country, '')
    ))
WHERE NULLIF(r.sale_product_id, '') IS NOT NULL
ORDER BY r.source_file, r.sale_product_id::integer, r.raw_row_id;

INSERT INTO dw.fact_sales (
    source_file,
    source_sale_id,
    date_key,
    customer_key,
    seller_key,
    product_key,
    store_key,
    sale_quantity,
    unit_price,
    sale_total_amount
)
SELECT
    r.source_file,
    r.id::integer AS source_sale_id,
    to_char(to_date(r.sale_date, 'MM/DD/YYYY'), 'YYYYMMDD')::integer AS date_key,
    c.customer_key,
    se.seller_key,
    p.product_key,
    st.store_key,
    r.sale_quantity::integer,
    r.product_price::numeric(12, 2),
    r.sale_total_price::numeric(12, 2)
FROM raw.mock_data r
JOIN dw.dim_customer c
    ON c.source_file = r.source_file
    AND c.source_customer_id = r.sale_customer_id::integer
JOIN dw.dim_seller se
    ON se.source_file = r.source_file
    AND se.source_seller_id = r.sale_seller_id::integer
JOIN dw.dim_product p
    ON p.source_file = r.source_file
    AND p.source_product_id = r.sale_product_id::integer
JOIN dw.dim_store st
    ON st.store_natural_key = md5(concat_ws('|',
        coalesce(r.store_name, ''),
        coalesce(r.store_location, ''),
        coalesce(r.store_city, ''),
        coalesce(r.store_state, ''),
        coalesce(r.store_country, ''),
        coalesce(r.store_phone, ''),
        coalesce(r.store_email, '')
    ))
WHERE NULLIF(r.id, '') IS NOT NULL;

CREATE OR REPLACE VIEW mart.sales_by_month AS
SELECT
    d.year,
    d.month,
    count(*) AS sales_count,
    sum(f.sale_quantity) AS units_sold,
    sum(f.sale_total_amount) AS total_sales_amount
FROM dw.fact_sales f
JOIN dw.dim_date d ON d.date_key = f.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

CREATE OR REPLACE VIEW mart.sales_by_category AS
SELECT
    pc.category_name AS product_category,
    pet.category_name AS pet_category,
    count(*) AS sales_count,
    sum(f.sale_quantity) AS units_sold,
    sum(f.sale_total_amount) AS total_sales_amount
FROM dw.fact_sales f
JOIN dw.dim_product p ON p.product_key = f.product_key
JOIN dw.dim_product_category pc ON pc.product_category_key = p.product_category_key
JOIN dw.dim_pet_category pet ON pet.pet_category_key = p.pet_category_key
GROUP BY pc.category_name, pet.category_name
ORDER BY pc.category_name, pet.category_name;
