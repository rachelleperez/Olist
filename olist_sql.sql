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
	MIN(o.order_purchase_timestamp) + interval '182.5 day' AS six_months_from_first_order,
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
	SUM(((oi.price * pc.quantity) + oi.freight_value)) OVER (PARTITION BY o.order_id)
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







