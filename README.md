# BigDataSnowflake

Лабораторная работа по предмету «Анализ больших данных»: преобразование денормализованных CSV-данных о зоомагазине в аналитическую схему PostgreSQL в формате snowflake/star.

## Что есть в репозитории

- Исходные CSV-файлы находятся в папке `исходные данные/`.
- PostgreSQL запускается в Docker через `docker-compose.yml`.
- SQL-скрипты инициализации находятся в папке `sql/` и автоматически выполняются при первом запуске базы.
- Сначала данные загружаются в `raw.mock_data`.
- Затем создаются аналитические таблицы в схеме `dw`.
- Для примеров аналитических запросов используются представления в схеме `mart`.

## Исходные данные

В репозитории находится 10 CSV-файлов по 1000 строк в каждом.  
Во всех файлах одинаковая денормализованная структура: в одной строке содержатся данные о клиенте, продавце, товаре, магазине, поставщике и продаже.

Важно: в некоторых строках поле `product_description` содержит многострочный текст в кавычках.  
Для загрузки используется PostgreSQL `COPY ... WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"')`, поэтому стандартный CSV-формат, включая многострочные поля, обрабатывается корректно.

В raw-таблице хранятся два разных идентификатора:

- `source_row_number` — физический номер строки внутри загруженного CSV-файла, начиная с 1 после заголовка;
- `id` — идентификатор продажи из самого CSV-файла.

## Целевая модель

Центральная таблица хранилища — `dw.fact_sales`.

Гранулярность факта:  
одна строка в `dw.fact_sales` соответствует одной строке продажи из одного исходного CSV-файла.

Бизнес-ключ строки факта:

```text
source_file + source_sale_id
```

Числовые идентификаторы в CSV-файлах локальны для каждого файла.  
Например, в каждом файле значения `id` идут от `1` до `1000`, поэтому если использовать только `id`, то 10000 исходных строк ошибочно схлопнутся в 1000 ключей.  
Поэтому `source_file` специально сохраняется в бизнес-ключах фактов и измерений там, где используются source id.

Используемые измерения:

- `dw.dim_date`
- `dw.dim_customer`
- `dw.dim_seller`
- `dw.dim_store`
- `dw.dim_product`
- `dw.dim_supplier`
- `dw.dim_product_category`
- `dw.dim_pet_category`

Таблица `dw.dim_product` ссылается на измерения поставщика, категории товара и категории питомца, поэтому схема имеет snowflake-структуру без лишнего усложнения.

Так как source id повторяются между 10 файлами, в фактах и измерениях вместе с ними сохраняется `source_file`. Это позволяет не объединять по ошибке строки из разных CSV-файлов.

Ожидаемые размеры измерений для этого набора данных:

- `dw.dim_customer`: `10000`
- `dw.dim_seller`: `10000`
- `dw.dim_product`: `10000`
- `dw.dim_store`: `10000`
- `dw.dim_supplier`: `10000`
- `dw.dim_product_category`: `3`
- `dw.dim_pet_category`: `5`

Большие размеры основных измерений здесь ожидаемы, так как сгенерированные строки содержат уникальные данные по клиентам, продавцам, товарам, магазинам и поставщикам.  
Повторно используются в основном только категории, вынесенные в отдельные snowflake-измерения.

## Запуск

Требования:

- Docker
- Docker Compose

Запуск PostgreSQL и инициализация базы:

```bash
docker compose up -d
```

SQL-скрипты выполняются автоматически только при первом создании Docker volume.  
Чтобы полностью пересоздать базу с нуля:

```bash
docker compose down -v
docker compose up -d
```

## Параметры подключения

Для подключения через DBeaver или другой SQL-клиент:

- Host: `localhost`
- Port: `5432`
- Database: `petstore_dw`
- User: `postgres`
- Password: `postgres`

## Порядок выполнения скриптов

PostgreSQL автоматически выполняет скрипты из папки `sql/` в следующем порядке:

1. `01_raw_staging.sql` — создает `raw.mock_data` и загружает все CSV-файлы.
2. `02_dw_schema.sql` — создает таблицу фактов и таблицы измерений.
3. `03_dw_load.sql` — загружает данные из raw-слоя в аналитическую схему.
4. `04_verification.sql` — выполняет проверки качества данных после загрузки.

## Проверка результата

Проверка количества строк в raw-таблице:

```sql
SELECT count(*) AS raw_rows
FROM raw.mock_data;
```

Ожидаемый результат: `10000`.

Проверка количества строк по каждому исходному файлу:

```sql
SELECT source_file, count(*) AS rows_loaded
FROM raw.mock_data
GROUP BY source_file
ORDER BY source_file;
```

Для каждого CSV-файла должно быть `1000` строк.

Проверка количества строк в фактовой таблице:

```sql
SELECT count(*) AS fact_rows
FROM dw.fact_sales;
```

Ожидаемый результат: `10000`.

Проверка размеров измерений:

```sql
SELECT 'customers' AS object_name, count(*) FROM dw.dim_customer
UNION ALL
SELECT 'sellers', count(*) FROM dw.dim_seller
UNION ALL
SELECT 'products', count(*) FROM dw.dim_product
UNION ALL
SELECT 'stores', count(*) FROM dw.dim_store
UNION ALL
SELECT 'suppliers', count(*) FROM dw.dim_supplier
UNION ALL
SELECT 'product_categories', count(*) FROM dw.dim_product_category
UNION ALL
SELECT 'pet_categories', count(*) FROM dw.dim_pet_category;
```

Ожидаемый результат:

```text
customers: 10000
sellers: 10000
products: 10000
stores: 10000
suppliers: 10000
product_categories: 3
pet_categories: 5
```

Запуск проверок качества с хост-машины:

```bash
docker exec -it bdsnowflake-postgres psql -U postgres -d petstore_dw -f /docker-entrypoint-initdb.d/04_verification.sql
```

Если запускать через DBeaver, нужно открыть файл `sql/04_verification.sql` и выполнить его.  
В результате в таблице `quality_checks` все проверки должны вернуть `PASS`:

- нет потери строк между `raw.mock_data` и `dw.fact_sales`
- нет дублей бизнес-ключа факта
- нет `NULL` в обязательных связях факта
- нет orphan foreign keys
- нет дублей бизнес-ключей в измерениях

Пример аналитического запроса:

```sql
SELECT *
FROM mart.sales_by_category
ORDER BY total_sales_amount DESC;
```

Еще один полезный запрос:

```sql
SELECT *
FROM mart.sales_by_month
ORDER BY year, month;
```

## Остановка

Остановить базу данных:

```bash
docker compose down
```

Удалить volume и все загруженные данные:

```bash
docker compose down -v
```
