/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

--===================================================
--Creating the customers dimension in the gold schema
--===================================================
CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	CASE WHEN ca.gen IS NULL OR ca.gen = 'n/a' THEN ci.cst_gndr
		 WHEN ca.gen <> ci.cst_gndr THEN ca.gen
	     ELSE ci.cst_gndr
	END AS gender,
	ci.cst_marital_status AS marital_status,
	cl.cntry AS country,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
	
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 cl
ON ci.cst_key = cl.cid;

--===================================================
--Creating the products dimension in the gold schema
--===================================================

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pd.prd_start_dt, pd.cat_id) AS product_key,
	pd.prd_id product_id,
	pd.prd_key AS product_number,
	pd.prd_nm AS product_name,
	pd.prd_line AS product_line,
	pd.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance AS maintanance,
	pd.prd_cost AS cost,
	pd.prd_start_dt AS start_date
FROM silver.crm_prd_info pd
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pd.cat_id = pc.id
WHERE prd_end_dt IS NULL --filtering the historical data;

--===================================================
--Creating the sales fact table in the gold schema
--===================================================
CREATE VIEW gold.fact_sales AS
SELECT 
	sls_ord_num AS order_number,
	cs.customer_key,
	pd.product_key,
	sls_order_dt AS order_date,
	sls_ship_dt AS  shipping_date,
	sls_due_dt AS due_date,
	sls_sales AS  sales_amount,
	sls_quantity AS quantity,
	sls_price AS price
FROM silver.crm_sales_details sls
LEFT JOIN gold.dim_customers cs
ON sls.sls_cust_id = cs.customer_id
LEFT JOIN gold.dim_products pd
ON sls.sls_prd_key = pd.product_number
