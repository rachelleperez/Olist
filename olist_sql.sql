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

SELECT c.customer_unique_id, 
	MIN(o.order_purchase_timestamp) as first_order_date, 
	MIN(o.order_purchase_timestamp) + interval '182.5 day' AS six_months_interfrom_first_order,
	COUNT(o.order_id)
FROM Orders o INNER JOIN Customers c USING(customer_id)
GROUP BY c.customer_unique_id
ORDER BY count DESC;

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


-- SET CLIENT_ENCODING TO 'utf8';

-- Master Joint ++ Geo Location


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
GROUP BY c.customer_unique_id), 

eight_table_merge AS (
SELECT * 
FROM sellers 
	INNER JOIN seven_table_merge USING(seller_id)
	INNER JOIN first_6_months USING(customer_unique_id))

SELECT *, 
	g1.geolocation_lat AS customer_geo_lat, 
	g1.geolocation_ing AS customer_geo_lng, 
	g1.geolocation_city AS customer_geo_city,
	g1.geolocation_state AS customer_geo_state,
	g2.geolocation_lat AS seller_geo_lat, 
	g2.geolocation_ing AS seller_geo_lng, 
	g2.geolocation_city AS seller_geo_city,
	g2.geolocation_state AS seller_geo_state

FROM geolocation g1 
	INNER JOIN eight_table_merge e ON g1.geolocation_zip_code_prefix = e.customer_zip_code
	INNER JOIN geolocation g2 ON g2.geolocation_zip_code_prefix = e.seller_zip_code_prefix
LIMIT 1;

/*

The following queries check if there if the city and state fields in the customer records directly match the ones based off zip code in the geolocation. 
They do not match. 
So, both records will be kept for the Python upload. 
However, any correlation analysis between sellers and customers will be based off the generated fields

*/

olist=# SELECT COUNT(*) FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix WHERE c.customer_city <> g.geolocation_city;
 count
--------
 954042
(1 row)


olist=# SELECT COUNT(*) FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix WHERE c.customer_city = g.geolocation_city;
  count
----------
 14129413
(1 row)


olist=# SELECT COUNT(*) FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix WHERE c.customer_state <>g.geolocation_state;
 count
-------
    74
(1 row)


olist=# SELECT COUNT(*) FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix WHERE c.customer_state = g.geolocation_state;
  count
----------
 15083381
(1 row)

-- To confirm than an order count based on the orders table is accurate (no dupes)

olist=# SELECT COUNT(Order_id) FROM orders;
 count
-------
 99441
(1 row)


olist=# SELECT COUNT(DISTINCT order_id) FROM orders;
 count
-------
 99441
(1 row)

-- Basic Info 

* Total Customers =  96096

olist=# SELECT COUNT(DISTINCT customer_unique_id) FROM customers INNER JOIN orders USING(customer_id);
 count
-------
 96096
(1 row)

* Total Orders = 99441

olist=# SELECT COUNT(DISTINCT order_id) FROM orders;
 count
-------
 99441
(1 row)


* Total Sellers = 3095

olist=# SELECT COUNT(DISTINCT seller_id) FROM sellers INNER JOIN order_items USING(seller_id);
 count
-------
  3095
(1 row)

-- How to get total prices per order? Better info by product! 

SELECT * FROM customers INNER JOIN orders USING (customer_id) INNER JOIN order_items USING(order_id);

        order_id             |          
		 customer_id            |        
		 customer_unique_id        | 
		 customer_zip_code |         
		  customer_city           | 
		  customer_state | 
		  order_status | 
		  order_purchase_timestamp |  
		  order_approved_at  | 
		  order_delivered_carrier_date | 
		  order_delivered_customer_date | 
		  order_estimated_delivery_date | 
		  order_item_id |            
		  product_id            |            
		  seller_id             | 
		  shipping_limit_date |  
		  price  | 
		  freight_value

WITH product_count AS (
	SELECT order_id, product_id, COUNT(product_id) AS quantity
	FROM customers INNER JOIN orders USING (customer_id) INNER JOIN order_items USING(order_id)
	GROUP BY order_id, product_id)

SELECT o.order_id, 
	pc.product_id, 
	oi.price, pc.quantity, 
	oi.freight_value, 
	(oi.price * pc.quantity) + oi.freight_value AS total_by_product, 
	((oi.price * pc.quantity) + oi.freight_value)) OVER (PARTITION BY o.order_id)
FROM order_items oi INNER JOIN orders o USING(order_id) INNER JOIN product_count pc USING (order_id)
ORDER BY order_id, product_id

-- Assumption - Customer pays for shipping
-- TOTALS by order
-- NEED to add customer_unique_id ++ AFTER establish reference tables
-- Review freight cost OR ignore freight cost moving forward as this is not revenue for Olist, just money that goes out.

WITH product_count AS (
	SELECT order_id, product_id, COUNT(product_id) AS quantity
	FROM customers INNER JOIN orders USING (customer_id) INNER JOIN order_items USING(order_id)
	GROUP BY order_id, product_id)

SELECT o.order_id,
	SUM(oi.price * pc.quantity) OVER (PARTITION BY o.order_id) AS subtotal,
	SUM(oi.freight_value) OVER (PARTITION BY o.order_id) AS shipping_cost,
	SUM((oi.price * pc.quantity) + oi.freight_value) OVER (PARTITION BY o.order_id) AS total
FROM order_items oi INNER JOIN orders o USING(order_id) INNER JOIN product_count pc USING (order_id)
ORDER BY o.order_id;

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


-- Using 6-month table to connect

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
       sum
------------------
 13591643.7000074
(1 row)


olist=# SELECT SUM(freight_value) FROM orders INNER JOIN order_items USING(order_id) INNER JOIN products USING(product_id);
       sum
------------------
 2251909.53999995
(1 row)


olist=# SELECT SUM(payment_value) FROM order_payments;
       sum
------------------
 16008872.1199988
(1 row)

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

 count
-------
   502
(1 row)

SELECT 

olist=# SELECT COUNT(DISTINCT customer_unique_id) FROM customers INNER JOIN orders USING(customer_id);
 count
-------
 96096
(1 row)

-- Only 502 FROM 96,096 total customers made 1 order within 6 months and later made another. Only 0.5%. Not sigficant. 

-- Min/Max Dates = First and last orders in dataset

olist=# SELECT MAX(order_purchase_timestamp) FROM orders;
         max
---------------------
 2018-10-17 17:30:18
(1 row)


olist=# SELECT MIN(order_purchase_timestamp) FROM orders;
         min
---------------------
 2016-09-04 21:15:19
(1 row)

-- How many customers have a first order within the last 6 months?

WITH brand_new_customers AS(

SELECT *, (SELECT MAX(order_purchase_timestamp) FROM orders) - interval '182.5 day' AS Last_6_months_in_data
FROM customers 
	INNER JOIN orders USING(customer_id)
	INNER JOIN order_payments USING(order_id)
WHERE order_purchase_timestamp > ((SELECT MAX(order_purchase_timestamp) FROM orders) - interval '182.5 day')
)

SELECT COUNT (DISTINCT customer_unique_id) FROM brand_new_customers;

-------
 28427
(1 row)


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

 count
-------
 28112
(1 row)

-- Are 30% of customers brand new! Should they be dropped?

-- How many orders are cancelled? Should these be removed?

olist=# SELECT DISTINCT order_status FROM orders;
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


olist=# SELECT COUNT(DISTINCT order_id) FROM orders WHERE order_status = 'canceled';
 count
-------
   625
(1 row)


olist=# SELECT COUNT(DISTINCT order_id) FROM orders;
 count
-------
 99441
(1 row)

-- Only 0.06% of orders are canceled. 

-- But, are they adding to the payments total? YES

SELECT SUM(payment_value) FROM orders INNER JOIN order_payments USING(order_id) WHERE order_status = 'canceled';
   sum
----------
 143255.6
(1 row)

-- Because they add to payment totals, these will be removed

-- df1 = Update to DF , removing cancelled orders. Unavailable will be left as these were real orders the customer wanted and is potential revenue the customer was willing to pay.

.
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


-- Create view #1

CREATE VIEW df2 AS 

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

          customer_id            |        customer_unique_id        | customer_zip_code |          customer_city           | customer_state
----------------------------------+----------------------------------+-------------------+----------------------------------+----------------

geolocation_zip_code_prefix |    geolocation_lat    |  geolocation_ing  |            geolocation_city            | geolocation_state
-----------------------------+-----------------------+-------------------+----------------------------------------+-------------------
                        1037 |     -23.5456212811527 | -46.6392920480017 | sao paulo                              | SP

SELECT DISTINCT customer_unique_id, customer_zip_code, geolocation_city AS customer_geo_city, geolocation_state AS customer_geo_state
FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix
ORDER BY customer_unique_id;


\COPY (SELECT DISTINCT customer_unique_id, customer_zip_code, geolocation_city AS customer_geo_city, geolocation_state AS customer_geo_state FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix) TO 'C:/Users/rache/DATA/Olist/GITHUB_OLIST/customer_demographics.csv' CSV HEADER;

SELECT COUNT(DISTINCT customer_id) 
FROM customers c INNER JOIN geolocation g ON c.customer_zip_code = g.geolocation_zip_code_prefix;

-- Does the count matches the rows in the Python upload?
-- No, is zip code data in the geolocation table distinct? - NO

olist=# SELECT COUNT(DISTINCT geolocation_zip_code_prefix) FROM geolocation;
 count
-------
 19015
(1 row)


olist=# SELECT COUNT(geolocation_state) FROM geolocation;
  count
---------
 1000163
(1 row)


olist=# SELECT COUNT(DISTINCT geolocation_state) FROM geolocation;
 count
-------
    27
(1 row)

-- The name of the city variates BUT the state doesn't so we will only add state data. 


-- ADDING CUSTOMER demographic

df1

       customer_unique_id        |  
	   date_first_order   | 
	   year_first_order | 
	   month_first_order | 
	   total_orders_first_6_months | 
	   total_paid_first_6_months
----------------------------------+---------------------+------------------+-------------------+-----------------------------+---------------------------

-- DF2 saved! 

        customer_unique_id        |  date_first_order   | year_first_order | month_first_order | total_orders_first_6_months | total_paid_first_6_months
----------------------------------+---------------------+------------------+-------------------+-----------------------------+---------------------------

-- Now, let's add geolocation


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

--

-- clean geo table with unique zip_code

CREATE VIEW geo_clean AS
SELECT 
	DISTINCT ON 
	(geolocation_zip_code_prefix) geolocation_zip_code_prefix, 
	geolocation_state, 
	geolocation_lat, 
	geolocation_ing
FROM geolocation;

-- join df2 with geo_clean AS df3 

CREATE VIEW df3 AS

WITH customer_plus_geo AS(
SELECT c.customer_unique_id, 
		gc.geolocation_zip_code_prefix AS customer_geo_city, 
		gc.geolocation_state AS customer_geo_state,
		gc.geolocation_lat AS customer_geo_lat,
		gc.geolocation_ing AS customer_geo_lng
FROM geo_clean gc INNER JOIN customers c ON c.customer_zip_code = gc.geolocation_zip_code_prefix)

SELECT * FROM customer_plus_geo INNER JOIN df2 USING(customer_unique_id);

-- view with orders first 6 months

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



-- now, let's add number of orders unavailable - df4

CREATE VIEW df4 AS 
WITH orders_status_bol_total AS(
SELECT customer_unique_id, order_id, order_status, CASE WHEN order_status = 'unavailable' THEN TRUE ELSE NULL END AS order_unavailable_bol
FROM customers INNER JOIN orders USING(customer_id)
),

customers_unavailable_count AS (

SELECT customer_unique_id, COUNT(order_unavailable_bol) AS order_count_unavailable
FROM orders_status_bol_total INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id
ORDER BY 2)

SELECT * FROM df3 INNER JOIN customers_unavailable_count USING(customer_unique_id);

-- number of days between approved and purchase

           order_id             |         
		     customer_id            |
			  order_status | 
			  order_purchase_timestamp |
			    order_approved_at  |
				 order_delivered_carrier_date | 
				 order_delivered_customer_date |
				  order_estimated_delivery_date

WITH orders_plus_approval_wait AS(
SELECT C.customer_unique_id, O.order_id, O.order_purchase_timestamp, o.order_approved_at, (o.order_approved_at - o.order_purchase_timestamp) as wait_for_seller_approval
FROM orders o INNER JOIN customers c USING(customer_id)
)

SELECT customer_unique_id, AVG(wait_for_seller_approval) AS avg_wait_seller_approval
FROM orders_plus_approval_wait INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id;

-- create d5

CREATE VIEW df5 AS 
WITH orders_plus_payment_approval_wait AS(
SELECT C.customer_unique_id, O.order_id, O.order_purchase_timestamp, o.order_approved_at, (o.order_approved_at - o.order_purchase_timestamp) as wait_for_payment_approval
FROM orders o INNER JOIN customers c USING(customer_id)
),

customers_plus_payment_approval_wait AS (

SELECT customer_unique_id, AVG(wait_for_payment_approval) AS avg_payment_processing_time
FROM orders_plus_payment_approval_wait INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id)

SELECT * FROM df4 INNER JOIN customers_plus_payment_approval_wait USING(customer_unique_id);


-- confirm view created

olist=# \d df5
                                     View "public.df5"
           Column            |            Type             | Collation | Nullable | Default
-----------------------------+-----------------------------+-----------+----------+---------
 customer_unique_id          | character varying(50)       |           |          |
 customer_geo_city           | integer                     |           |          |
 customer_geo_state          | character varying(10)       |           |          |
 customer_geo_lat            | double precision            |           |          |
 customer_geo_lng            | double precision            |           |          |
 date_first_order            | timestamp without time zone |           |          |
 year_first_order            | double precision            |           |          |
 month_first_order           | double precision            |           |          |
 total_orders_first_6_months | bigint                      |           |          |
 total_paid_first_6_months   | double precision            |           |          |
 order_count_unavailable     | bigint                      |           |          |
 avg_payment_processing_time | interval                    |           |          |


-- now, add lag between order date and seller delivery to carrier (avg_seller processing time)

CREATE VIEW df6  AS
WITH orders_plus_seller_processing_time AS(
SELECT C.customer_unique_id, O.order_id, O.order_purchase_timestamp, o.order_approved_at, (o.order_delivered_carrier_date- o.order_purchase_timestamp) as seller_processing_time
FROM orders_6m o6 INNER JOIN orders o USING(order_id) INNER JOIN customers c USING(customer_id)
),

customers_plus_seller_processing_time AS (

SELECT customer_unique_id, AVG(seller_processing_time) AS avg_seller_processing_time
FROM orders_plus_seller_processing_time
GROUP BY customer_unique_id)

SELECT * FROM df5 INNER JOIN customers_plus_seller_processing_time USING(customer_unique_id);

-- add transit_time

CREATE VIEW df7 AS
WITH orders_plus_transit_time AS(
SELECT C.customer_unique_id, O.order_id, (o.order_delivered_customer_date-o.order_delivered_carrier_date) as transit_time
FROM orders_6m o6 INNER JOIN orders o USING(order_id) INNER JOIN customers c USING(customer_id)
),

customers_plus_transit_time AS (

SELECT customer_unique_id, AVG(transit_time) AS avg_transit_time
FROM orders_plus_transit_time
GROUP BY customer_unique_id)

SELECT * FROM df6 INNER JOIN customers_plus_transit_time USING(customer_unique_id);

-- add order_lead_time for customer

CREATE VIEW df8 AS
WITH orders_plus_lead_time AS(
SELECT C.customer_unique_id, O.order_id, (o.order_delivered_customer_date-o.order_purchase_timestamp) as lead_time
FROM orders_6m o6 INNER JOIN orders o USING(order_id) INNER JOIN customers c USING(customer_id)
),

customers_plus_avg_lead_time AS (

SELECT customer_unique_id, AVG(lead_time) AS avg_lead_time
FROM orders_plus_lead_time
GROUP BY customer_unique_id)

SELECT * FROM df7 INNER JOIN customers_plus_avg_lead_time USING(customer_unique_id);

-- add number of items per order

CREATE VIEW df9 AS
WITH items_per_order AS (
SELECT customer_unique_id, order_id, COUNT(order_item_id) AS item_count
FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_items USING(order_id)
GROUP BY customer_unique_id, order_id), 

avg_item_count_per_order AS (

SELECT customer_unique_id, AVG(item_count) AS avg_item_count_per_order
FROM items_per_order INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id)

SELECT * FROM df8 INNER JOIN avg_item_count_per_order USING (customer_unique_id);

-- add number of products per order (distinct product)

CREATE VIEW df10 AS
WITH product_count_per_order AS (
SELECT customer_unique_id, order_id, COUNT(DISTINCT product_id) AS product_count
FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_items USING(order_id)
GROUP BY customer_unique_id, order_id), 

avg_product_count_per_order AS (

SELECT customer_unique_id, AVG(product_count) AS avg_product_count_per_order
FROM product_count_per_order INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id)

SELECT * FROM df9 INNER JOIN avg_product_count_per_order USING (customer_unique_id);

-- change layoyt

\x on

-- Do customers missing their shipping deadline correlate with LTV?

CREATE VIEW df11 AS
WITH orders_shipped_past_deadline AS (
SELECT *
FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_items USING(order_id)
WHERE  shipping_limit_date > order_delivered_carrier_date),

order_shipped_late AS(

SELECT customer_unique_id, COUNT(DISTINCT order_id) as orders_shipped_late
FROM orders_shipped_past_deadline INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id)

SELECT * FROM df10 INNER JOIN order_shipped_late USING(customer_unique_id);

-- number of items  grouped by product, and order number

CREATE VIEW df12 AS
WITH units_per_product_by_customer AS(
SELECT customer_unique_id, product_id, COUNT(product_id) AS units_per_product
FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_items USING(order_id) INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id, product_id),

avg_quantity_by_product AS (

SELECT customer_unique_id, AVG(units_per_product) AS avg_quantity_by_product
FROM units_per_product_by_customer
GROUP BY customer_unique_id)

SELECT * FROM df11 INNER JOIN avg_quantity_by_product USING(customer_unique_id);

-- average price per product

CREATE VIEW df13 AS 
WITH price_per_unit_by_customer AS
(
SELECT customer_unique_id, AVG(price) AS average_price_per_unit
FROM customers INNER JOIN orders USING(customer_id) INNER JOIN order_items USING(order_id) INNER JOIN orders_6m USING(order_id)
GROUP BY customer_unique_id
)
SELECT * FROM df12 INNER JOIN price_per_unit_by_customer USING(customer_unique_id);

-- average freight cost per order, freight value column is grouped by products within an order.
-- If order has 2 units of product X and each have freight cost Y. Y is the freight price for the 2 units of the same product. Gets repeated in each line, not split.

CREATE VIEW df14 AS
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

SELECT * 
FROM df13 INNER JOIN customer_aggregates USING (customer_unique_id);

-- NOTE - How many orders were left out of analysis as they fall outsitde of first 6 months?
-- Less than 2%. OK. 98.76% of orders provided fall in customer's first 6 months.

olist=# SELECT COUNT(DISTINCT order_id) FROM orders_6m;
 count
-------
 98207
(1 row)


olist=# SELECT COUNT(DISTINCT order_id) FROM orders;
 count
-------
 99441
(1 row)

-- how many product categories were purchased from? - 71


SELECT COUNT(DISTINCT product_category_name_english)
FROM product_category_name_translation
	INNER JOIN products USING(product_category_name)
	INNER JOIN order_items USING(product_id)
	INNER JOIN orders_6m USING(order_id);\



-- Total order_count

 order_count_total
-------------------
             98207
(1 row)

-- order_count_by_category
-- SO far, in Excel did proportion of order_count to total_orders
-- The top 8 categories were (8/71 - 12%) were present in over 50% of the orders (51%) with bed_bad_table is almost 10% of orders


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
 count
-------
 95558
(1 row)

-- average price per category -- would looking at the correlation between LTV and top x categories be fair as some categorie have prices that are more expensive?
-- 

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

CREATE VIEW df15 AS
WITH customers_top_10_cat AS(
	SELECT DISTINCT customer_unique_id, 
			CASE WHEN product_category_name_english IN (SELECT product_category_name_english FROM top_10_product_categories)
			THEN 1
			ELSE 0
			END AS ordered_from_top_10_prod_category_bol
	FROM product_category_name_translation
			INNER JOIN products USING(product_category_name)
			INNER JOIN order_items USING(product_id)
			INNER JOIN orders USING(order_id)
			INNER JOIN customers USING(customer_id)
	ORDER BY 1
)
SELECT * FROM df14 
INNER JOIN customers_top_10_cat USING(customer_unique_id);

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

CREATE VIEW df16 AS
WITH customers_bol_seller_perfect_avg_review_score AS(
	SELECT DISTINCT customer_unique_id, 
			CASE WHEN seller_id IN (SELECT seller_id FROM sellers_perfect_review)
			THEN 1
			ELSE 0
			END AS ordered_from_seller_perfect_avg_review_bol
	FROM order_items
			INNER JOIN orders USING(order_id)
			INNER JOIN customers USING(customer_id)
	ORDER BY 2 DESC
)
SELECT * FROM df15 INNER JOIN customers_bol_seller_perfect_avg_review_score USING(customer_unique_id)
LIMIT 1;

-- seller --- ??

SELECT seller_id, COUNT(DISTINCT  state)
FROM order_items
GROUP BY seller_id
ORDER BY 2 DESC;



	



 