SELECT 'raw.mock_data' AS table_name, count(*) AS row_count FROM raw.mock_data;
SELECT 'dw.fact_sales' AS table_name, count(*) AS row_count FROM dw.fact_sales;

SELECT source_file, count(*) AS rows_loaded
FROM raw.mock_data
GROUP BY source_file
ORDER BY source_file;

WITH quality_checks AS (
    SELECT
        'raw_to_fact_row_count' AS check_name,
        CASE WHEN (SELECT count(*) FROM raw.mock_data) = (SELECT count(*) FROM dw.fact_sales)
            THEN 'PASS' ELSE 'FAIL' END AS status,
        format('raw=%s, fact=%s',
            (SELECT count(*) FROM raw.mock_data),
            (SELECT count(*) FROM dw.fact_sales)
        ) AS details

    UNION ALL
    SELECT
        'fact_business_key_duplicates',
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('duplicate_keys=%s', count(*))
    FROM (
        SELECT source_file, source_sale_id
        FROM dw.fact_sales
        GROUP BY source_file, source_sale_id
        HAVING count(*) > 1
    ) duplicates

    UNION ALL
    SELECT
        'fact_required_links_are_not_null',
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('bad_rows=%s', count(*))
    FROM dw.fact_sales
    WHERE date_key IS NULL
       OR customer_key IS NULL
       OR seller_key IS NULL
       OR product_key IS NULL
       OR store_key IS NULL

    UNION ALL
    SELECT
        'fact_orphan_date_keys',
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('orphans=%s', count(*))
    FROM dw.fact_sales f
    LEFT JOIN dw.dim_date d ON d.date_key = f.date_key
    WHERE d.date_key IS NULL

    UNION ALL
    SELECT
        'fact_orphan_customer_keys',
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('orphans=%s', count(*))
    FROM dw.fact_sales f
    LEFT JOIN dw.dim_customer d ON d.customer_key = f.customer_key
    WHERE d.customer_key IS NULL

    UNION ALL
    SELECT
        'fact_orphan_seller_keys',
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('orphans=%s', count(*))
    FROM dw.fact_sales f
    LEFT JOIN dw.dim_seller d ON d.seller_key = f.seller_key
    WHERE d.seller_key IS NULL

    UNION ALL
    SELECT
        'fact_orphan_product_keys',
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('orphans=%s', count(*))
    FROM dw.fact_sales f
    LEFT JOIN dw.dim_product d ON d.product_key = f.product_key
    WHERE d.product_key IS NULL

    UNION ALL
    SELECT
        'fact_orphan_store_keys',
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('orphans=%s', count(*))
    FROM dw.fact_sales f
    LEFT JOIN dw.dim_store d ON d.store_key = f.store_key
    WHERE d.store_key IS NULL

    UNION ALL
    SELECT
        'dim_customer_business_key_duplicates',
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('duplicate_keys=%s', count(*))
    FROM (
        SELECT source_file, source_customer_id
        FROM dw.dim_customer
        GROUP BY source_file, source_customer_id
        HAVING count(*) > 1
    ) duplicates

    UNION ALL
    SELECT
        'dim_seller_business_key_duplicates',
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('duplicate_keys=%s', count(*))
    FROM (
        SELECT source_file, source_seller_id
        FROM dw.dim_seller
        GROUP BY source_file, source_seller_id
        HAVING count(*) > 1
    ) duplicates

    UNION ALL
    SELECT
        'dim_product_business_key_duplicates',
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('duplicate_keys=%s', count(*))
    FROM (
        SELECT source_file, source_product_id
        FROM dw.dim_product
        GROUP BY source_file, source_product_id
        HAVING count(*) > 1
    ) duplicates

    UNION ALL
    SELECT
        'dim_store_business_key_duplicates',
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('duplicate_keys=%s', count(*))
    FROM (
        SELECT store_natural_key
        FROM dw.dim_store
        GROUP BY store_natural_key
        HAVING count(*) > 1
    ) duplicates

    UNION ALL
    SELECT
        'dim_supplier_business_key_duplicates',
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('duplicate_keys=%s', count(*))
    FROM (
        SELECT supplier_natural_key
        FROM dw.dim_supplier
        GROUP BY supplier_natural_key
        HAVING count(*) > 1
    ) duplicates

    UNION ALL
    SELECT
        'dim_category_duplicates',
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('duplicate_keys=%s', count(*))
    FROM (
        SELECT category_name
        FROM dw.dim_product_category
        GROUP BY category_name
        HAVING count(*) > 1
        UNION ALL
        SELECT category_name
        FROM dw.dim_pet_category
        GROUP BY category_name
        HAVING count(*) > 1
    ) duplicates
)
SELECT check_name, status, details
FROM quality_checks
ORDER BY check_name;

SELECT *
FROM mart.sales_by_category
ORDER BY total_sales_amount DESC
LIMIT 10;
