/* ASSIGNMENT 2 */
/* SECTION 2 */

-- COALESCE
/* 1. Our favourite manager wants a detailed long list of products, but is afraid of tables! 
We tell them, no problem! We can produce a list with all of the appropriate details. 

Using the following syntax you create our super cool and not at all needy manager a list:

SELECT 
product_name || ', ' || product_size|| ' (' || product_qty_type || ')'
FROM product

But wait! The product table has some bad data (a few NULL values). 
Find the NULLs and then using COALESCE, replace the NULL with a 
blank for the first problem, and 'unit' for the second problem. 

HINT: keep the syntax the same, but edited the correct components with the string. 
The `||` values concatenate the columns into strings. 
Edit the appropriate columns -- you're making two edits -- and the NULL rows will be fixed. 
All the other rows will remain the same.) */

select product_name || ', ' || 
coalesce(product_size,'')|| ' (' || 
coalesce(product_qty_type,'unit') || ')' as Detatiled_Product_List

from product


--Windowed Functions
/* 1. Write a query that selects from the customer_purchases table and numbers each customer’s  
visits to the farmer’s market (labeling each market date with a different number). 
Each customer’s first visit is labeled 1, second visit is labeled 2, etc. 

You can either display all rows in the customer_purchases table, with the counter changing on
each new market date for each customer, or select only the unique market dates per customer 
(without purchase details) and number those visits. 
HINT: One of these approaches uses ROW_NUMBER() and one uses DENSE_RANK(). */

SELECT 
customer_id
,market_date
,row_number()OVER(PARTITION BY customer_id ORDER BY market_date ASC) as Visit_Number
FROM customer_purchases
GROUP BY market_date, customer_id


/* 2. Reverse the numbering of the query from a part so each customer’s most recent visit is labeled 1, 
then write another query that uses this one as a subquery (or temp table) and filters the results to 
only the customer’s most recent visit. */

SELECT customer_id, market_date as most_recent_visit
FROM( 
	SELECT 
	customer_id
	,market_date
	,row_number()OVER(PARTITION BY customer_id ORDER BY market_date DESC) as Visit_Number
	FROM customer_purchases
	GROUP BY market_date, customer_id
)
WHERE visit_number = 1

/* 3. Using a COUNT() window function, include a value along with each row of the 
customer_purchases table that indicates how many different times that customer has purchased that product_id. */

SELECT 
customer_id
,product_id
,market_date
,transaction_time
,count(*)OVER(PARTITION BY customer_id,product_id) as Product_this_customer
FROM customer_purchases


-- String manipulations
/* 1. Some product names in the product table have descriptions like "Jar" or "Organic". 
These are separated from the product name with a hyphen. 
Create a column using SUBSTR (and a couple of other commands) that captures these, but is otherwise NULL. 
Remove any trailing or leading whitespaces. Don't just use a case statement for each product! 

| product_name               | description |
|----------------------------|-------------|
| Habanero Peppers - Organic | Organic     |

Hint: you might need to use INSTR(product_name,'-') to find the hyphens. INSTR will help split the column. */

SELECT 
product_id
,product_name
,CASE WHEN position = 0
	THEN NULL
	ELSE substr(clean_data,position+2)
	END as descripion

FROM(
	SELECT 
	product_id
	,product_name
	,rtrim(ltrim(product_name)) as clean_data
	,instr(product_name, '-') as position

	FROM product
)

/* 2. Filter the query to show any product_size value that contain a number with REGEXP. */

SELECT 
product_id
,product_name
,product_size
,CASE WHEN position = 0
	THEN NULL
	ELSE substr(clean_data,position+2)
	END as descripion

FROM(
	SELECT 
	product_id
	,product_name
	,product_size
	,rtrim(ltrim(product_name)) as clean_data
	,instr(product_name, '-') as position

	FROM product
)
WHERE product_size REGEXP '\d'

-- UNION
/* 1. Using a UNION, write a query that displays the market dates with the highest and lowest total sales.

HINT: There are a possibly a few ways to do this query, but if you're struggling, try the following: 
1) Create a CTE/Temp Table to find sales values grouped dates; */

DROP TABLE IF EXISTS total_sale_amount;
CREATE TEMP TABLE total_sale_amount AS 

SELECT DISTINCT market_date
,SUM(quantity * cost_to_customer_per_qty) OVER(PARTITION BY market_date) as daily_total
FROM customer_purchases;

/* 2) Create another CTE/Temp table with a rank windowed function on the previous query to create 
"best day" and "worst day"; */

DROP TABLE IF EXISTS rank_sale_amount;
CREATE TEMP TABLE rank_sale_amount AS 

SELECT DISTINCT market_date, daily_total
,ROW_NUMBER () OVER (ORDER BY daily_total DESC) as best_day
,ROW_NUMBER () OVER (ORDER BY daily_total ASC) as worst_day
FROM total_sale_amount;

/* 3) Query the second temp table twice, once for the best day, once for the worst day, 
with a UNION binding them. */

SELECT market_date, daily_total
FROM rank_sale_amount
WHERE best_day = 1

UNION

SELECT market_date, daily_total
FROM rank_sale_amount
WHERE worst_day = 1


/* SECTION 3 */

-- Cross Join
/*1. Suppose every vendor in the `vendor_inventory` table had 5 of each of their products to sell to **every** 
customer on record. How much money would each vendor make per product? 
Show this by vendor_name and product name, rather than using the IDs.

HINT: Be sure you select only relevant columns and rows. 
Remember, CROSS JOIN will explode your table rows, so CROSS JOIN should likely be a subquery. 
Think a bit about the row counts: how many distinct vendors, product names are there (x)?
How many customers are there (y). 
Before your final group by you should have the product of those two queries (x*y).  */

--DETERMINE WHICH PRODUCTS VENDOR SELL
DROP TABLE IF EXISTS temp_inv;
CREATE TEMP TABLE temp_inv AS

SELECT DISTINCT product_id
FROM vendor_inventory
GROUP BY product_id;
--ABOVE QUERY RETURNED THE SET OF PRODUCTS 1,2,3,4,5,7,8,16 SOLD BY THE VENDORS

--DETERMINE LOWEST PRICE CHARGED FOR EACH PRODUCT; NOT ALL CUSTOMERS WERE CHARGED THE SAME PRICE FOR THE SAME PRODUCT
DROP TABLE IF EXISTS price_inv;
CREATE TEMP TABLE price_inv AS
SELECT *

FROM(
	SELECT DISTINCT vendor_id
	,product_id
	,cost_to_customer_per_qty
	,row_number()OVER(PARTITION BY vendor_id, product_id ORDER BY cost_to_customer_per_qty ASC) as last_qty
	FROM customer_purchases
)x

WHERE x.last_qty = 1 
--THIS QUERY MAKES THE FIRST QUERY REDUNDANT

--ASSIGN TO EACH CUSTOMER THE PRODUCT SET ABOVE AND THE COST TO PURCHASE
SELECT DISTINCT vendor_id, product_id
,SUM(cost) OVER(PARTITION BY vendor_id, product_id) as total_profit
FROM (
	SELECT *, cost_to_customer_per_qty * 5 as cost
	FROM (
		SELECT *
		FROM customer
		CROSS JOIN price_inv
	)x
)y


-- INSERT
/*1.  Create a new table "product_units". 
This table will contain only products where the `product_qty_type = 'unit'`. 
It should use all of the columns from the product table, as well as a new column for the `CURRENT_TIMESTAMP`.  
Name the timestamp column `snapshot_timestamp`. */

DROP TABLE IF EXISTS product_units;
CREATE TEMP TABLE product_units AS 

SELECT product_id, product_name, product_size, product_category_id, product_qty_type, CURRENT_TIMESTAMP as snapshot_timestamp
FROM product
WHERE product_qty_type = 'unit'

/*2. Using `INSERT`, add a new row to the product_units table (with an updated timestamp). 
This can be any product you desire (e.g. add another record for Apple Pie). */

INSERT INTO temp.product_units
VALUES (27,'Banana Pie', 'large', 4, 'unit', CURRENT_TIMESTAMP) 


-- DELETE
/* 1. Delete the older record for the whatever product you added. 

HINT: If you don't specify a WHERE clause, you are going to have a bad time.*/

DELETE FROM product_units
WHERE product_id = 27

-- UPDATE
/* 1.We want to add the current_quantity to the product_units table. 
First, add a new column, current_quantity to the table using the following syntax. */

ALTER TABLE product_units
ADD current_quantity INT;

/* Then, using UPDATE, change the current_quantity equal to the last quantity value from the vendor_inventory details.

HINT: This one is pretty hard. 
First, determine how to get the "last" quantity per product. */

--create temp table to sort data on the product current inventory

DROP TABLE IF EXISTS current_inv;
CREATE TEMP TABLE current_inv AS 

SELECT *

FROM (
	SELECT *
	, row_number()OVER(PARTITION BY vendor_id, product_id ORDER BY market_date DESC) as last_qty
	FROM vendor_inventory 
)x

WHERE x.last_qty = 1 

/* Second, coalesce null values to 0 (if you don't have null values, figure out how to rearrange your query so you do.) 
--spoke with James on Friday and he instructed to ignore this step

Third, SET current_quantity = (...your select statement...), remembering that WHERE can only accommodate one column. 
Finally, make sure you have a WHERE statement to update the right row, 
	you'll need to use product_units.product_id to refer to the correct row within the product_units table. 
When you have all of these components, you can run the update statement. */

--determine which rows in the products unit we have inventory data for
SELECT product_units.product_id
,product_name
,product_size
,snapshot_timestamp
,current_quantity
,quantity

FROM product_units
INNER JOIN current_inv
	ON product_units.product_id = current_inv.product_id

--there are only 6 rows, so update only those 6 rows
UPDATE product_units
SET  current_quantity = CASE
	WHEN product_id = 3 THEN 60
	WHEN product_id = 4 THEN 30
	WHEN product_id = 5 THEN 20
	WHEN product_id = 7 THEN 10
	WHEN product_id = 16 THEN 140
	WHEN product_id = 8 THEN 10
END
WHERE product_id IN (3,4,5,7,16,8)

