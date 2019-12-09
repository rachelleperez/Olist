CREATE TABLE order_items (
	order_id varchar NOT NULL,
	order_item_id varchar (50),
	product_id varchar (50),
	seller_id varchar (50),
	shipping_limit_date timestamp,
	price float,
	freight_value float) 
	
SELECT * FROM order_items;

COPY order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price,freight_value)
FROM 'C:\Program Files\PostgreSQL\11\data\olist\olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM order_items;

CREATE TABLE order_payments (
	order_id varchar(50), 
	payment_sequential int, 
	payment_type varchar (50), 
	payment_installments int, 
	payment_value float)
	
SELECT * FROM order_payments

COPY order_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value)
FROM 'C:\Program Files\PostgreSQL\11\data\olist\olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM order_payments;

CREATE TABLE customers (
	customer_id varchar (50) NOT NULL PRIMARY KEY,
	customer_unique_id varchar(50),
	customer_zip_code int,
	customer_city varchar (50),
	customer_state varchar (50)
	)
	
SELECT * FROM customers;

COPY customers (customer_id, customer_unique_id, customer_zip_code, customer_city, customer_state)
FROM 'C:\Program Files\PostgreSQL\11\data\olist\olist_customers_dataset.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM customers;

DROP TABLE geolocation

CREATE TABLE geolocation
	(geolocation_zip_code_prefix int,
	 geolocation_lat float,
	 geolocation_Ing float,
	 geolocation_city varchar (50),
	 geolocation_state varchar (10)
	)
	
SELECT * FROM geolocation;

COPY geolocation (geolocation_zip_code_prefix, geolocation_lat, geolocation_Ing, geolocation_city, geolocation_state)
FROM 'C:\Program Files\PostgreSQL\11\data\olist\olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM geolocation;

CREATE TABLE reviews
	 (review_id varchar (50) NOT NULL,
	 order_id varchar (50), 
	 review_score int, 
	 review_comment_title varchar (50), 
	 review_comment_message varchar (500), 
	 review_creation_date timestamp, 
	 review_answer_timestamp timestamp)

SELECT * FROM reviews;

COPY reviews (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
FROM 'C:\Program Files\PostgreSQL\11\data\olist\olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM reviews;

CREATE TABLE orders (
	order_id VARCHAR(100) PRIMARY KEY,
	customer_id VARCHAR(100),
	order_status VARCHAR(50),
	order_purchase_timestamp TIMESTAMP,
	order_approved_at TIMESTAMP,
	order_delivered_carrier_date TIMESTAMP,
	order_delivered_customer_date TIMESTAMP,
	order_estimated_delivery_date TIMESTAMP
	) 

SELECT * FROM orders;

COPY orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date,order_estimated_delivery_date)
FROM 'C:\Program Files\PostgreSQL\11\data\olist\olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM orders;

CREATE TABLE products (
	product_id varchar(50) NOT NULL PRIMARY KEY,
	product_category_name VARCHAR(70),
	product_name_length int ,
	product_description_length int,
	product_photos_qty int,
	product_weight_g int ,
	product_length_cm int ,
	product_height_cm int ,
	product_width_cm int ) 

SELECT * FROM products;

COPY products (product_id, product_category_name, product_name_length, product_description_length, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
FROM 'C:\Program Files\PostgreSQL\11\data\olist\olist_products_dataset.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM products;

CREATE TABLE sellers
	(seller_id varchar(50) NOT NULL PRIMARY KEY,
	seller_zip_code_prefix int,
	seller_city varchar (100) ,
	seller_state varchar(15)
	)

SELECT * FROM sellers

COPY sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
FROM 'C:\Program Files\PostgreSQL\11\data\olist\olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM sellers

CREATE TABLE product_category_name_translation (
	product_category_name varchar(50),
	product_category_name_english varchar(70)
	)
	
SELECT * FROM product_category_name_translation 

COPY product_category_name_translation (product_category_name, product_category_name_english)
FROM 'C:\Program Files\PostgreSQL\11\data\olist\product_category_name_translation.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM product_category_name_translation 

/* 

Generate date for fi

*/

-- first_6_months

SELECT c.customer_unique_id, 
	MIN(o.order_purchase_timestamp) as first_order_date, 
	MIN(o.order_purchase_timestamp) + interval '182.5 day' AS six_months_from_first_order
FROM Orders o INNER JOIN Customers c USING(customer_id)
GROUP BY c.customer_unique_id
ORDER BY c.customer_unique_id

-- Master Joint, only missing geolocation

WITH seven_table_merge AS (
SELECT * 
 FROM product_category_name_translation 
 	INNER JOIN products USING(product_category_name) 
	INNER JOIN order_items USING(product_id)
	INNER JOIN reviews USING(order_id) 
	INNER JOIN orders o USING (order_id) 
	INNER JOIN order_payments USING(order_id) 
	INNER JOIN orders o2 USING(order_id)
	INNER JOIN customers C ON C.customer_id = o.customer_id),

first_6_months AS (
SELECT c.customer_unique_id, 
	MIN(o.order_purchase_timestamp) as first_order_date, 
	MIN(o.order_purchase_timestamp) + interval '182.5 day' AS six_months_from_first_order,
	COUNT(order_id) AS order_count
FROM Orders o INNER JOIN Customers c USING(customer_id)
GROUP BY c.customer_unique_id)


SELECT * 
FROM sellers 
	INNER JOIN seven_table_merge USING(seller_id)
	INNER JOIN first_6_months USING(customer_unique_id);

-- 

SELECT DISTINCT order_status FROM orders;

/*
 order_status
--------------
 shipped
 unavailable
 invoiced
 created
 approved
 processing
 delivered
 canceled
(8 rows)
*/



/*

The following queries check if there if the city and state fields in the customer records directly match the ones based off zip code in the geolocation. 
They do not match. 
So, both records will be kept for the Python upload. 
However, any correlation analysis between sellers and customers will be based off the generated fields

*/

SELECT COUNT(*) FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix WHERE c.customer_city <> g.geolocation_city;

/*

count
--------
 954042
(1 row)

*/
SELECT COUNT(*) FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix WHERE c.customer_city = g.geolocation_city;
/*
  count
----------
 14129413
(1 row)
*/

olist=# SELECT COUNT(*) FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix WHERE c.customer_state <>g.geolocation_state;
/* count
-------
    74
(1 row)
*/

olist=# SELECT COUNT(*) FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix WHERE c.customer_state = g.geolocation_state;
/*  count
----------
 15083381
(1 row)
*/
-- To confirm than an order count based on the orders table is accurate (no dupes)

olist=# SELECT COUNT(Order_id) FROM orders;
/* count
-------
 99441
(1 row)
*/

olist=# SELECT COUNT(DISTINCT order_id) FROM orders;
/* count
-------
 99441
(1 row)
*/

-- Basic Info 

-- Total Customers =  96096

olist=# SELECT COUNT(DISTINCT customer_unique_id) FROM customers INNER JOIN orders USING(customer_id);
/*
count
-------
 96096
(1 row)
*/
* Total Orders = 99441

olist=# SELECT COUNT(DISTINCT order_id) FROM orders;

/* 
count
-------
 99441
(1 row)
*/

-- Total Sellers = 3095

olist=# SELECT COUNT(DISTINCT seller_id) FROM sellers INNER JOIN order_items USING(seller_id);
/* count
-------
  3095
(1 row)
*/

-- First 6 Months


WITH first_6_months_dates AS(
SELECT c.customer_unique_id, 
	MIN(o.order_purchase_timestamp) as first_order_date, 
	MIN(o.order_purchase_timestamp) + interval '182.5 day' AS six_months_from_first_order
FROM Orders o INNER JOIN Customers c USING(customer_id)
GROUP BY c.customer_unique_id
ORDER BY c.customer_unique_id)

SELECT *
FROM first_6_months_dates
	INNER JOIN customers USING(customer_unique_id)
	INNER JOIN orders USING(customer_id)
	INNER JOIN order_payments USING(order_id)
WHERE order_purchase_timestamp < six_months_from_first_order;

-- First 6 month grouped by customer

WITH first_6_months_dates AS(
SELECT c.customer_unique_id, 
	MIN(o.order_purchase_timestamp) as first_order_date, 
	MIN(o.order_purchase_timestamp) + interval '182.5 day' AS six_months_from_first_order
FROM Orders o INNER JOIN Customers c USING(customer_id)
GROUP BY c.customer_unique_id
ORDER BY c.customer_unique_id), 

first_6_months_activity AS (

SELECT *
FROM first_6_months_dates
	INNER JOIN customers USING(customer_unique_id)
	INNER JOIN orders USING(customer_id)
	INNER JOIN order_payments USING(order_id)
WHERE order_purchase_timestamp < six_months_from_first_order)


SELECT 
	customer_unique_id,
	MIN(order_purchase_timestamp) AS date_first_order,
	EXTRACT(YEAR FROM MIN(order_purchase_timestamp)) AS year_first_order,
	EXTRACT(MONTH FROM MIN(order_purchase_timestamp)) AS month_first_order,
	COUNT(DISTINCT order_id) AS total_orders_first_6_months,
	SUM(payment_value) AS total_paid_first_6_months
FROM first_6_months_activity
GROUP BY customer_unique_id
ORDER BY customer_unique_id;


-- What value should I use for revenue? Does calculated total match payment value columns



olist=# SELECT SUM(price) FROM orders INNER JOIN order_items USING(order_id) INNER JOIN products USING(product_id);
/*       sum
------------------
 13591643.7000074
(1 row)
*/

olist=# SELECT SUM(freight_value) FROM orders INNER JOIN order_items USING(order_id) INNER JOIN products USING(product_id);
/*       sum
------------------
 2251909.53999995
(1 row)
*/

olist=# SELECT SUM(payment_value) FROM order_payments;
/*
       sum
------------------
 16008872.1199988
(1 row)
*/

-- Do Customer that make one order in 6 months, end up making orders later or remain inactive?

WITH first_6_months_dates AS(
SELECT c.customer_unique_id, 
	MIN(o.order_purchase_timestamp) as first_order_date, 
	MIN(o.order_purchase_timestamp) + interval '182.5 day' AS six_months_from_first_order
FROM Orders o INNER JOIN Customers c USING(customer_id)
GROUP BY c.customer_unique_id
ORDER BY c.customer_unique_id), 

first_6_months_activity AS (

SELECT *
FROM first_6_months_dates
	INNER JOIN customers USING(customer_unique_id)
	INNER JOIN orders USING(customer_id)
	INNER JOIN order_payments USING(order_id)
WHERE order_purchase_timestamp < six_months_from_first_order), 

totals_by_customer AS (

SELECT *
FROM customers
	INNER JOIN orders USING(customer_id)
	INNER JOIN order_payments USING(order_id)
)

SELECT 
	f.customer_unique_id,
	MIN(f.order_purchase_timestamp) AS date_first_order,
	COUNT(DISTINCT t.order_id) AS total_orders,
	COUNT(DISTINCT f.order_id) AS total_orders_first_6_months,
	COUNT(DISTINCT t.order_id) - COUNT(DISTINCT f.order_id) AS Count_difference,
	SUM(t.payment_value) AS total_paid,
	SUM(f.payment_value) AS total_paid_first_6_months,
	SUM(t.payment_value) - SUM(f.payment_value) AS Rev_Difference
FROM first_6_months_activity f INNER JOIN totals_by_customer t USING(customer_unique_id)
GROUP BY f.customer_unique_id
HAVING COUNT(DISTINCT f.order_id) = 1 AND (COUNT(DISTINCT t.order_id) - COUNT(DISTINCT f.order_id)) <> 0
ORDER BY Count_difference DESC;

-- COUNT the number of customers who make 1 order in the first 6 months and LATER make another order.

WITH first_6_months_dates AS(
SELECT c.customer_unique_id, 
	MIN(o.order_purchase_timestamp) as first_order_date, 
	MIN(o.order_purchase_timestamp) + interval '182.5 day' AS six_months_from_first_order
FROM Orders o INNER JOIN Customers c USING(customer_id)
GROUP BY c.customer_unique_id
ORDER BY c.customer_unique_id), 

first_6_months_activity AS (

SELECT *
FROM first_6_months_dates
	INNER JOIN customers USING(customer_unique_id)
	INNER JOIN orders USING(customer_id)
	INNER JOIN order_payments USING(order_id)
WHERE order_purchase_timestamp < six_months_from_first_order), 

totals_by_customer AS (

SELECT *
FROM customers
	INNER JOIN orders USING(customer_id)
	INNER JOIN order_payments USING(order_id)
), 

reworked_customer_aggregates AS (

SELECT 
	f.customer_unique_id,
	MIN(f.order_purchase_timestamp) AS date_first_order,
	COUNT(DISTINCT t.order_id) AS total_orders,
	COUNT(DISTINCT f.order_id) AS total_orders_first_6_months,
	COUNT(DISTINCT t.order_id) - COUNT(DISTINCT f.order_id) AS Count_difference,
	SUM(t.payment_value) AS total_paid,
	SUM(f.payment_value) AS total_paid_first_6_months,
	SUM(t.payment_value) - SUM(f.payment_value) AS Rev_Difference
FROM first_6_months_activity f INNER JOIN totals_by_customer t USING(customer_unique_id)
GROUP BY f.customer_unique_id
HAVING COUNT(DISTINCT f.order_id) = 1 AND (COUNT(DISTINCT t.order_id) - COUNT(DISTINCT f.order_id)) <> 0
ORDER BY Count_difference DESC)

SELECT COUNT (*) 
FROM reworked_customer_aggregates;

/*
count
-------
   502
(1 row)
*/

SELECT COUNT(DISTINCT customer_unique_id) FROM customers INNER JOIN orders USING(customer_id);
 
/*
count
-------
 96096
(1 row)
*/

-- Only 502 FROM 96,096 total customers made 1 order within 6 months and later made another. Only 0.5%. Not sigficant. 

-- Min/Max Dates = First and last orders in dataset

SELECT MAX(order_purchase_timestamp) FROM orders;
/*
         max
---------------------
 2018-10-17 17:30:18
(1 row)
*/

SELECT MIN(order_purchase_timestamp) FROM orders;

/*         min
---------------------
 2016-09-04 21:15:19
(1 row)
*/

-- How many customers have a first order within the last 6 months?

WITH brand_new_customers AS(

SELECT *, (SELECT MAX(order_purchase_timestamp) FROM orders) - interval '182.5 day' AS Last_6_months_in_data
FROM customers 
	INNER JOIN orders USING(customer_id)
	INNER JOIN order_payments USING(order_id)
WHERE order_purchase_timestamp > ((SELECT MAX(order_purchase_timestamp) FROM orders) - interval '182.5 day')
)

SELECT COUNT (DISTINCT customer_unique_id) FROM brand_new_customers;
/*
-------
 28427
(1 row)
*/

WITH min_date AS

(SELECT customer_unique_id, MIN(order_purchase_timestamp)
FROM customers 
	INNER JOIN orders USING(customer_id)
GROUP BY customer_unique_id)

SELECT COUNT(DISTINCT m.customer_unique_id)
FROM min_date m
	INNER JOIN customers USING(customer_unique_id)
	INNER JOIN orders USING(customer_id)
	INNER JOIN order_payments USING(order_id)
WHERE min >  CAST('2018-04-17' AS timestamp);

/*
count
-------
 28112
(1 row)
*/

-- Are 30% of customers brand new! Should they be dropped?

-- How many orders are cancelled? Should these be removed?

olist=# SELECT DISTINCT order_status FROM orders;

/*
order_status
--------------
 shipped
 unavailable
 invoiced
 created
 approved
 processing
 delivered
 canceled
(8 rows)
*/

olist=# SELECT COUNT(DISTINCT order_id) FROM orders WHERE order_status = 'canceled';

/*
 count
-------
   625
(1 row)
*/

olist=# SELECT COUNT(DISTINCT order_id) FROM orders;

/*
count
-------
 99441
(1 row)
*/

-- Only 0.06% of orders are canceled. 

-- But, are they adding to the payments total? YES

SELECT SUM(payment_value) FROM orders INNER JOIN order_payments USING(order_id) WHERE order_status = 'canceled';

/* sum
----------
 143255.6
(1 row)
*/

-- Because they add to payment totals, these will be removed

-- Create base df (df01) with basic customer aggregates (excludes orders cancelled and not within first 6 months)

CREATE VIEW df01 AS 

WITH first_6_months_dates AS(
SELECT c.customer_unique_id, 
	MIN(o.order_purchase_timestamp) as first_order_date, 
	MIN(o.order_purchase_timestamp) + interval '182.5 day' AS six_months_from_first_order
FROM Orders o INNER JOIN Customers c USING(customer_id)
GROUP BY c.customer_unique_id
ORDER BY c.customer_unique_id), 

first_6_months_activity AS (

SELECT *
FROM first_6_months_dates
	INNER JOIN customers USING(customer_unique_id)
	INNER JOIN orders USING(customer_id)
	INNER JOIN order_payments USING(order_id)
WHERE order_purchase_timestamp < six_months_from_first_order AND order_status <> 'canceled')


SELECT 
	customer_unique_id,
	MIN(order_purchase_timestamp) AS date_first_order,
	EXTRACT(YEAR FROM MIN(order_purchase_timestamp)) AS year_first_order,
	EXTRACT(MONTH FROM MIN(order_purchase_timestamp)) AS month_first_order,
	COUNT(DISTINCT order_id) AS total_orders_first_6_months,
	SUM(payment_value) AS total_paid_first_6_months
FROM first_6_months_activity
GROUP BY customer_unique_id
ORDER BY customer_unique_id;

-- Start creating pieces to add to df. Now customer demographic

SELECT DISTINCT customer_unique_id, customer_zip_code, geolocation_city AS customer_geo_city, geolocation_state AS customer_geo_state
FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix
ORDER BY customer_unique_id;


\COPY (SELECT DISTINCT customer_unique_id, customer_zip_code, geolocation_city AS customer_geo_city, geolocation_state AS customer_geo_state FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix) TO 'C:/Users/rache/DATA/Olist/GITHUB_OLIST/customer_demographics.csv' CSV HEADER;

SELECT COUNT(DISTINCT customer_id) 
FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix;

-- Does the count matches the rows in the Python upload?
-- No, is zip code data in the geolocation table distinct? - NO

SELECT COUNT(DISTINCT geolocation_zip_code_prefix) FROM geolocation;

/*
count
-------
 19015
(1 row)
*/

SELECT COUNT(geolocation_state) FROM geolocation;

/*
count
---------
 1000163
(1 row)
*/

SELECT COUNT(DISTINCT geolocation_state) FROM geolocation;
/*
 count
-------
    27
(1 row)
*/

-- The name of the city variates BUT the state doesn't so we will only add state data. 

-- Now, add geolocation

WITH first_6_months_dates AS(
SELECT c.customer_unique_id, 
	MIN(o.order_purchase_timestamp) as first_order_date, 
	MIN(o.order_purchase_timestamp) + interval '182.5 day' AS six_months_from_first_order
FROM Orders o INNER JOIN Customers c USING(customer_id)
GROUP BY c.customer_unique_id
ORDER BY c.customer_unique_id), 

first_6_months_activity AS 
(
SELECT *
FROM first_6_months_dates
	INNER JOIN customers USING(customer_unique_id)
	INNER JOIN orders USING(customer_id)
	INNER JOIN order_payments USING(order_id)
WHERE order_purchase_timestamp < six_months_from_first_order AND order_status <> 'canceled'),

df1 AS 
(
SELECT 
	customer_unique_id,
	customer_zip_code,
	MIN(order_purchase_timestamp) AS date_first_order,
	EXTRACT(YEAR FROM MIN(order_purchase_timestamp)) AS year_first_order,
	EXTRACT(MONTH FROM MIN(order_purchase_timestamp)) AS month_first_order,
	COUNT(DISTINCT order_id) AS total_orders_first_6_months,
	SUM(payment_value) AS total_paid_first_6_months
FROM first_6_months_activity
GROUP BY customer_unique_id, customer_zip_code
ORDER BY customer_unique_id)

SELECT df1.customer_unique_id, df1.customer_zip_code AS customer_geo_zip, g.geolocation_state AS customer_geo_state, g.geolocation_lat AS customer_geo_lat, g.geolocation_ing AS customer_geo_lng
FROM df1 INNER JOIN (SELECT DISTINCT geolocation_state, geolocation_zip_code_prefix, geolocation_lat, geolocation_ing FROM geolocation) AS g 
ON df1.customer_zip_code = g.geolocation_zip_code_prefix;


-- clean geo table with unique zip_code

CREATE VIEW geo_clean AS
SELECT 
	DISTINCT ON 
	(geolocation_zip_code_prefix) geolocation_zip_code_prefix, 
	geolocation_state, 
	geolocation_lat, 
	geolocation_ing
FROM geolocation;

/*
-- add customer geo location

CREATE VIEW ds3 AS
WITH customer_plus_geo AS(
SELECT DISTINCT c.customer_unique_id, 
		gc.geolocation_zip_code_prefix AS customer_geo_zip,
		gc.geolocation_state AS customer_geo_state,
		gc.geolocation_lat AS customer_geo_lat,
		gc.geolocation_ing AS customer_geo_lng
FROM customers c LEFT JOIN geo_clean gc ON c.customer_zip_code = gc.geolocation_zip_code_prefix)

SELECT COUNT(customer_unique_id) FROM customer_plus_geo

SELECT DISTINCT * FROM df2 LEFT JOIN customer_plus_geo USING(customer_unique_id);

-- list - orders first 6 months

CREATE VIEW orders_6m AS

WITH first_6_months_dates AS(
SELECT c.customer_unique_id, 
	MIN(o.order_purchase_timestamp) as first_order_date, 
	MIN(o.order_purchase_timestamp) + interval '182.5 day' AS six_months_from_first_order
FROM Orders o INNER JOIN Customers c USING(customer_id)
GROUP BY c.customer_unique_id
ORDER BY c.customer_unique_id)

SELECT DISTINCT order_id
FROM first_6_months_dates
	INNER JOIN customers USING(customer_unique_id)
	INNER JOIN orders USING(customer_id)
WHERE order_purchase_timestamp < six_months_from_first_order AND order_status <> 'canceled';

*/

-- add number of orders unavailable

CREATE VIEW df02 AS 
WITH orders_status_bol_total AS(
SELECT customer_unique_id, order_id, order_status, CASE WHEN order_status = 'unavailable' THEN TRUE ELSE NULL END AS order_unavailable_bol
FROM customers INNER JOIN orders USING(customer_id)
),

customers_unavailable_count AS (

SELECT customer_unique_id, COUNT(order_unavailable_bol) AS order_count_unavailable
FROM orders_status_bol_total INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id
ORDER BY 2)

SELECT * FROM df01 LEFT JOIN customers_unavailable_count USING(customer_unique_id);


-- add payment approval time

CREATE VIEW df03 AS 
WITH orders_plus_payment_approval_wait AS(
SELECT C.customer_unique_id, O.order_id, O.order_purchase_timestamp, o.order_approved_at, (o.order_approved_at - o.order_purchase_timestamp) as wait_for_payment_approval
FROM orders o INNER JOIN customers c USING(customer_id)
),

customers_plus_payment_approval_wait AS (

SELECT customer_unique_id, AVG(wait_for_payment_approval) AS avg_payment_processing_time
FROM orders_plus_payment_approval_wait INNER JOIN orders_6m USING(order_id)
WHERE wait_for_payment_approval >= '0 microsecond'
GROUP BY customer_unique_id)

SELECT * FROM df02 LEFT JOIN customers_plus_payment_approval_wait USING(customer_unique_id);


-- now, add lag between order date and seller delivery to carrier (avg_seller processing time)

CREATE VIEW df04  AS
WITH orders_plus_seller_processing_time AS(
SELECT C.customer_unique_id, O.order_id, O.order_purchase_timestamp, o.order_approved_at, (o.order_delivered_carrier_date- o.order_purchase_timestamp) as seller_processing_time
FROM orders_6m o6 INNER JOIN orders o USING(order_id) INNER JOIN customers c USING(customer_id)
),

customers_plus_seller_processing_time AS (

SELECT customer_unique_id, AVG(seller_processing_time) AS avg_seller_processing_time
FROM orders_plus_seller_processing_time
WHERE seller_processing_time >= '0 microsecond'
GROUP BY customer_unique_id)

SELECT * FROM df03 LEFT JOIN customers_plus_seller_processing_time USING(customer_unique_id);

-- add transit_time

CREATE VIEW df05 AS
WITH orders_plus_transit_time AS(
SELECT C.customer_unique_id, O.order_id, (o.order_delivered_customer_date-o.order_delivered_carrier_date) as transit_time
FROM orders_6m o6 INNER JOIN orders o USING(order_id) INNER JOIN customers c USING(customer_id)
),

customers_plus_transit_time AS (

SELECT customer_unique_id, AVG(transit_time) AS avg_transit_time
FROM orders_plus_transit_time
WHERE transit_time >= '0 microsecond'
GROUP BY customer_unique_id)

SELECT * FROM df04 LEFT JOIN customers_plus_transit_time USING(customer_unique_id);

-- add order_lead_time for customer

CREATE VIEW df06 AS
WITH orders_plus_lead_time AS(
SELECT C.customer_unique_id, O.order_id, (o.order_delivered_customer_date-o.order_purchase_timestamp) as lead_time
FROM orders_6m o6 INNER JOIN orders o USING(order_id) INNER JOIN customers c USING(customer_id)
),

customers_plus_avg_lead_time AS (

SELECT customer_unique_id, AVG(lead_time) AS avg_lead_time
FROM orders_plus_lead_time
WHERE lead_time >= '0 microsecond'
GROUP BY customer_unique_id)

SELECT * FROM df05 LEFT JOIN customers_plus_avg_lead_time USING(customer_unique_id);

-- add number of items per order

CREATE VIEW df07 AS
WITH items_per_order AS (
SELECT customer_unique_id, order_id, COUNT(order_item_id) AS item_count
FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_items USING(order_id)
GROUP BY customer_unique_id, order_id), 

avg_item_count_per_order AS (

SELECT customer_unique_id, AVG(item_count) AS avg_item_count_per_order
FROM items_per_order INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id)

SELECT * FROM df06 LEFT JOIN avg_item_count_per_order USING (customer_unique_id);

-- add number of products per order (distinct product)

CREATE VIEW df08 AS
WITH product_count_per_order AS (
SELECT customer_unique_id, order_id, COUNT(DISTINCT product_id) AS product_count
FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_items USING(order_id)
GROUP BY customer_unique_id, order_id), 

avg_product_count_per_order AS (

SELECT customer_unique_id, AVG(product_count) AS avg_product_count_per_order
FROM product_count_per_order INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id)

SELECT * FROM df07 LEFT JOIN avg_product_count_per_order USING (customer_unique_id);

-- change layoyt

\x on

-- Do customers missing their shipping deadline correlate with LTV?

CREATE VIEW df09 AS
WITH orders_shipped_past_deadline AS (
SELECT *
FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_items USING(order_id)
WHERE  shipping_limit_date > order_delivered_carrier_date),

order_shipped_late AS(

SELECT customer_unique_id, COUNT(DISTINCT order_id) as orders_shipped_late
FROM orders_shipped_past_deadline INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id)

SELECT * FROM df08 LEFT JOIN order_shipped_late USING(customer_unique_id);

-- number of items  grouped by product, and order number

CREATE VIEW df010 AS
WITH units_per_product_by_customer AS(
SELECT customer_unique_id, product_id, COUNT(product_id) AS units_per_product
FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_items USING(order_id) INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id, product_id),

avg_quantity_by_product AS (

SELECT customer_unique_id, AVG(units_per_product) AS avg_quantity_by_product
FROM units_per_product_by_customer
GROUP BY customer_unique_id)

SELECT * FROM df09 LEFT JOIN avg_quantity_by_product USING(customer_unique_id);

-- average price per product

CREATE VIEW df011 AS 
WITH price_per_unit_by_customer AS
(
SELECT customer_unique_id, AVG(price) AS average_price_per_unit
FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_items USING(order_id) INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id
)
SELECT * FROM df010 LEFT JOIN price_per_unit_by_customer USING(customer_unique_id);

-- average freight cost per order, freight value column is grouped by products within an order.
-- If order has 2 units of product X and each have freight cost Y. Y is the freight price for the 2 units of the same product. Gets repeated in each line, not split.

CREATE VIEW df012 AS
WITH freight_costs_corrected AS (
	SELECT DISTINCT customer_unique_id, order_id, product_id, freight_value
	FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_items USING(order_id) INNER JOIN orders_6m USING(order_id)
),

customer_order_aggregates AS (
SELECT customer_unique_id, order_id, SUM(freight_value) AS order_freight_cost
FROM freight_costs_corrected
GROUP BY customer_unique_id, order_id), 

customer_aggregates AS (

SELECT customer_unique_id, AVG(order_freight_cost) AS avg_freight_cost_per_order
FROM customer_order_aggregates
GROUP BY customer_unique_id)

SELECT * FROM df011 LEFT JOIN customer_aggregates USING (customer_unique_id);

-- NOTE - How many orders were left out of analysis as they fall outside of first 6 months?
-- Less than 2%. OK. 98.76% of orders provided fall in customer's first 6 months.

olist=# SELECT COUNT(DISTINCT order_id) FROM orders_6m;
/*
 count
-------
 98207
(1 row)
*/

olist=# SELECT COUNT(DISTINCT order_id) FROM orders;
 
/* 
 count
-------
 99441
(1 row)
*/

-- how many product categories were purchased from? - 71

SELECT COUNT(DISTINCT product_category_name_english)
FROM product_category_name_translation
	INNER JOIN products USING(product_category_name)
	INNER JOIN order_items USING(product_id)
	INNER JOIN orders_6m USING(order_id);\

-- Total order_count

/*
 order_count_total
-------------------
             98207
(1 row)
*/

-- order_count_by_category
-- SO far, in Excel did proportion of order_count to total_orders
-- The top 7 categories were were present in over 50% of the orders (51%) with bed_bad_table is almost 10% of orders. 


	SELECT product_category_name_english, COUNT(DISTINCT order_id) AS order_count_by_category
	FROM product_category_name_translation
		INNER JOIN products USING(product_category_name)
		INNER JOIN order_items USING(product_id)
		INNER JOIN orders_6m USING(order_id)
		INNER JOIN order_payments USING(order_id)
	GROUP BY product_category_name_english
	ORDER BY 2 DESC

-- NEXT: Boolean, did customer purchase from top 8 categories
-- PROBLEM - How to make it even across customers. Will customers that have 1 order be less likely to have diversity.
-- How to boil down info at customer level. 

-- number of customers grouped by category

SELECT product_category_name_english, COUNT(DISTINCT customer_unique_id) AS customer_count_by_category
	FROM product_category_name_translation
		INNER JOIN products USING(product_category_name)
		INNER JOIN order_items USING(product_id)
		INNER JOIN orders USING(order_id)
		INNER JOIN customers USING(customer_id)
	WHERE order_id IN (SELECT * FROM orders_6m) 
	GROUP BY product_category_name_english
	ORDER BY 2 DESC

-- total customers

olist=# select count(distinct customer_unique_id) from customers inner join orders using(customer_id) inner join orders_6m using(order_id);
/*
count
-------
 95558
(1 row)
*/

-- average price per category -- would looking at the correlation between LTV and top x categories be fair as some categorie have prices that are more expensive?
 
SELECT product_category_name_english, AVG(price) AS avg_price_per_product
	FROM product_category_name_translation
		INNER JOIN products USING(product_category_name)
		INNER JOIN order_items USING(product_id)
		INNER JOIN order_payments USING(order_id)
	GROUP BY product_category_name_english
	ORDER BY 2 DESC

-- top 10 categories based on number of customers 

CREATE VIEW top_10_product_categories AS
SELECT product_category_name_english, COUNT(DISTINCT customer_unique_id) AS customer_count_by_category
	FROM product_category_name_translation
		INNER JOIN products USING(product_category_name)
		INNER JOIN order_items USING(product_id)
		INNER JOIN orders USING(order_id)
		INNER JOIN customers USING(customer_id)
	WHERE order_id IN (SELECT * FROM orders_6m) 
	GROUP BY product_category_name_english
	ORDER BY 2 DESC
	LIMIT 10;

-- create boolean column for ordered from top 10 prod categories

CREATE VIEW df013 AS
WITH customers_top_10_cat AS(
	SELECT DISTINCT customer_unique_id, order_id, product_id, product_category_name_english,
			CASE WHEN product_category_name_english IN (SELECT product_category_name_english FROM top_10_product_categories)
			THEN 1
			ELSE 0
			END AS top_10_cat_bol
	FROM product_category_name_translation
			RIGHT JOIN products USING(product_category_name)
			RIGHT JOIN order_items USING(product_id)
			RIGHT JOIN orders USING(order_id)
			RIGHT JOIN customers USING(customer_id)
	WHERE order_id IN (SELECT * FROM orders_6m)
), 
customer_product_count_top_10_cat AS(
	SELECT customer_unique_id, sum(top_10_cat_bol) AS sum_bol
	FROM customers_top_10_cat
	GROUP BY customer_unique_id), 

customer_bol_cat AS (

	SELECT customer_unique_id, CASE WHEN sum_bol = 0 THEN 0 ELSE 1 END AS ordered_from_top_10_prod_category_bol 
	FROM customer_product_count_top_10_cat
)

SELECT * FROM df012 LEFT JOIN customer_bol_cat USING(customer_unique_id);


-- # sellers per customer

SELECT customer_unique_id, COUNT(DISTINCT seller_id) as seller_count, COUNT(DISTINCT order_id)
FROM customers 
	INNER JOIN orders USING(customer_id)
	INNER JOIN order_items USING(order_id)
GROUP BY customer_unique_id
ORDER BY 2 DESC;

-- sellers by average review

CREATE VIEW sellers_perfect_review AS
WITH seller_order_review_scores AS (
	SELECT DISTINCT seller_id, order_id, review_score
	FROM customers 
		INNER JOIN orders USING(customer_id)
		INNER JOIN order_items USING(order_id)
		INNER JOIN reviews USING(order_id)
	WHERE order_id IN (SELECT * from orders_6m)
)
SELECT seller_id
FROM seller_order_review_scores
GROUP BY seller_id
HAVING AVG(review_score) = 5

-- customers_bol_sellers_perfect_rating

CREATE VIEW df014 AS
WITH customers_perfect_seller AS (
	SELECT DISTINCT customer_unique_id, order_id, product_id, seller_id,
			CASE WHEN seller_id IN (SELECT seller_id FROM sellers_perfect_review)
			THEN 1
			ELSE 0
			END AS perf_sell_bol
	FROM order_items RIGHT JOIN orders USING(order_id) RIGHT JOIN customers USING(customer_id)
	WHERE order_id IN (SELECT * FROM orders_6m)
), 
customer_count_perf_sel AS(
	SELECT customer_unique_id, SUM(perf_sell_bol) AS sum_bol
	FROM customers_perfect_seller
	GROUP BY customer_unique_id), 

customer_bol_sel AS (

	SELECT customer_unique_id, CASE WHEN sum_bol = 0 THEN 0 ELSE 1 END AS ordered_from_seller_perfect_avg_review_bol
	FROM customer_count_perf_sel
)

SELECT * FROM df013 LEFT JOIN customer_bol_sel USING(customer_unique_id);


-- avg payment processing time

SELECT AVG(avg_payment_processing_time) FROM df15;

/*      avg
-----------------
 10:13:43.460021
(1 row)
*/

-- avg seller processing time

olist=# SELECT AVG(avg_seller_processing_time) FROM df15;

/*          avg
------------------------
 2 days 14:51:12.469364
(1 row)
*/

-- add avg number of days it takes between they day the product was delivered and the date the survey was sent by Olist?
-- NOTE - some negative. Survey is sometimes sent BEFORE the product is delivered.

CREATE VIEW df015 AS
WITH orders_plus_survey_lag AS(
SELECT C.customer_unique_id, O.order_id, (r.review_creation_date-o.order_delivered_customer_date) as survey_lag
FROM reviews r INNER JOIN orders o USING(order_id) INNER JOIN customers c USING(customer_id)
WHERE order_id IN (SELECT * FROM orders_6m)
),

customers_plus_avg_survey_lag AS (

SELECT customer_unique_id, AVG(survey_lag) AS avg_survey_lag
FROM orders_plus_survey_lag
WHERE survey_lag >= '0 microsecond'
GROUP BY customer_unique_id)

SELECT * FROM df014 	LEFT JOIN customers_plus_avg_survey_lag USING(customer_unique_id);

-- how long does it take a customer to review after purchase?

CREATE VIEW df016 AS
WITH orders_plus_review_lag AS(
SELECT C.customer_unique_id, O.order_id, (r.review_answer_timestamp-o.order_delivered_customer_date) as review_lag
FROM reviews r INNER JOIN orders o USING(order_id) INNER JOIN customers c USING(customer_id)
WHERE order_id IN (SELECT * FROM orders_6m)
),

customers_plus_avg_review_lag AS (

SELECT customer_unique_id, AVG(review_lag) AS avg_review_lag
FROM orders_plus_review_lag
WHERE review_lag >= '0 microsecond'
GROUP BY customer_unique_id)

SELECT * FROM df015 LEFT JOIN customers_plus_avg_review_lag USING(customer_unique_id);

-- some review BEFORE delivery!!! -- Consider why would a review be left BEFORE the product was arrived? 
-- Removed from data.

WITH orders_plus_review_lag AS(
SELECT C.customer_unique_id, O.order_id, (r.review_answer_timestamp-o.order_delivered_customer_date) as review_lag
FROM reviews r INNER JOIN orders o USING(order_id) INNER JOIN customers c USING(customer_id)
WHERE order_id IN (SELECT * FROM orders_6m)
)

SELECT COUNT(order_id) FROM orders_plus_review_lag WHERE review_lag <= '0 microsecond';

/*count
-------
  4904
(1 row)
*/

-- average review rating per customer

CREATE VIEW df017 AS
WITH customers_plus_avg_review_score AS (
	SELECT customer_unique_id, AVG(review_score) AS avg_review_score
	FROM reviews r INNER JOIN orders o USING(order_id) INNER JOIN customers c USING(customer_id)
	WHERE order_id IN (SELECT * FROM orders_6m)
	GROUP BY customer_unique_id)

SELECT * from df016 INNER JOIN customers_plus_avg_review_score USING (customer_unique_id);

-- What method of payments are there?

SELECT DISTINCT payment_type FROM order_payments;

/*
 payment_type
--------------
 not_defined
 boleto
 debit_card
 voucher
 credit_card
(5 rows)
*/

SELECT payment_type, COUNT(DISTINCT order_id) AS order_count FROM order_payments GROUP BY payment_type ORDER BY order_count DESC;

/*
 payment_type | order_count
--------------+-------------
 credit_card  |       76505
 boleto       |       19784
 voucher      |        3866
 debit_card   |        1528
 not_defined  |           3
(5 rows)
*/

-- average number of payment types per order - Does this customer tend to break apart payments in different types?

WITH customer_order_payment AS (
	SELECT customer_unique_id, order_id, payment_type
	FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_payments USING(order_id)
	WHERE order_id IN (SELECT * FROM orders_6m)
),

order_plus_payment_type_count AS (
	SELECT customer_unique_id, order_id, COUNT(DISTINCT payment_type) as payment_type_count
	FROM customer_order_payment
	GROUP BY customer_unique_id, order_id
	ORDER BY 3 DESC
)

SELECT customer_unique_id, AVG(payment_type_count)
FROM order_plus_payment_type_count
GROUP BY customer_unique_id

-- number of orders per customer in which boleto or voucher was used for payment

CREATE VIEW df018 AS
WITH order_count_boleto_voucher AS (
	SELECT customer_unique_id, COUNT(order_id) AS order_count_boleto_voucher
	FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_payments USING(order_id)
	WHERE order_id IN (SELECT * FROM orders_6m) AND (payment_type = 'boleto' OR payment_type = 'voucher')
	GROUP BY customer_unique_id
)

SELECT * FROM df017 LEFT JOIN order_count_boleto_voucher USING(customer_unique_id);

-- numer of orders per customer paid by cc (credit or debit)

CREATE VIEW df019 AS
WITH order_count_card AS (
	SELECT customer_unique_id, COUNT(order_id) AS order_count_card
	FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_payments USING(order_id)
	WHERE order_id IN (SELECT * FROM orders_6m) AND (payment_type = 'credit_card' OR payment_type = 'debit_card')
	GROUP BY customer_unique_id
)

SELECT * FROM df018 LEFT JOIN order_count_card USING(customer_unique_id);

-- avg number of installments by payment_id (grouped by )

CREATE VIEW df020 AS
WITH installments_by_order AS (
	SELECT customer_unique_id, order_id, SUM(payment_installments) AS installments_per_order
	FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_payments USING(order_id)
	WHERE order_id IN (SELECT * FROM orders_6m)
	GROUP BY customer_unique_id, order_id),

avg_installments_by_customer AS (
	SELECT customer_unique_id, AVG(installments_per_order) AS avg_installments
	FROM installments_by_order
	GROUP BY customer_unique_id)

SELECT * FROM df019 LEFT JOIN avg_installments_by_customer USING(customer_unique_id);

-- df finished! 

-- collect customer state

\COPY (SELECT * FROM df020) TO 'C:/Users/rache/DATA/Olist/GITHUB_OLIST/main_df.csv' CSV HEADER;

CREATE VIEW cgeo1 AS
SELECT DISTINCT customer_unique_id, customer_state, customer_zip_code
FROM customers INNER JOIN orders USING(customer_id)
WHERE order_id IN (SELECT * FROM orders_6m) AND order_status <> 'canceled';


\COPY (SELECT * FROM cgeo1) TO 'C:/Users/rache/DATA/Olist/GITHUB_OLIST/customer_demographics.csv' CSV HEADER;

-- df_adjustment as timedelta (interval) is not readible in OLS regression model

CREATE VIEW df021 AS
SELECT *, 
	EXTRACT(DAY FROM avg_payment_processing_time) AS avg_days_payment_processing_time,
	EXTRACT(DAY FROM avg_seller_processing_time) AS avg_days_seller_processing_time,
	EXTRACT(DAY FROM avg_transit_time) AS avg_days_transit_time,
	EXTRACT(DAY FROM avg_lead_time) AS avg_days_lead_time,
	EXTRACT(DAY FROM avg_survey_lag) AS avg_days_survey_lag,
	EXTRACT(DAY FROM avg_review_lag) AS avg_daysreview_lag
FROM df020;

\COPY (SELECT * FROM df021) TO 'C:/Users/rache/DATA/Olist/GITHUB_OLIST/main_df.csv' CSV HEADER;




