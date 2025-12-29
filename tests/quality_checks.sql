/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading bronze Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
--=====================================================
-- Check for NULLS or Duplicates in the primary_key
--=====================================================

SELECT * 
FROM(
	SELECT 
	*,
	RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM silver.crm_cust_info) t
WHERE flag_last = 1 AND cst_id IS NOT NULL;

--Check for unwanted spaces
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE TRIM(cst_firstname) != cst_firstname;

--SOLUTION 
SELECT TRIM(cst_firstname)
FROM silver.crm_cust_info;

--Check for unwanted spaces
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE TRIM(cst_lastname) != cst_lastname;

--SOLUTION 
SELECT TRIM(cst_lastname) cst_lastname
FROM silver.crm_cust_info;

--quality checks
--check for consistency in columns with low cardinality
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status,
CASE WHEN cst_marital_status = UPPER(TRIM('S')) THEN 'Single'
     WHEN cst_marital_status = UPPER(TRIM('M')) THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status
FROM silver.crm_cust_info;

--quality checks
--check for consistency in columns with low cardinality
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_gndr,
CASE WHEN cst_gndr = UPPER(TRIM('F')) THEN 'Female'
     WHEN cst_gndr = UPPER(TRIM('M')) THEN 'Male'
	 ELSE 'n/a'
END cst_gndr
FROM silver.crm_cust_info;

--Checking whether its a date or not
SELECT ISDATE(2025-11-15) AS valid_date;

---Checking for unusual columns
SELECT cst_create_date
FROM silver.crm_cust_info
WHERE cst_create_date > GETDATE();

SELECT * FROM silver.crm_cust_info;

--====================================================
--Quality Checks for crm_prd_info
--====================================================

SELECT * 
FROM(
	SELECT 
	*,
	RANK() OVER(PARTITION BY prd_id ORDER BY prd_start_dt DESC) AS flag_last
	FROM bronze.crm_prd_info) t
WHERE flag_last > 1 OR prd_id IS NULL;

--Divide and separate values in the column prd_key

SELECT *,
UPPER(REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')) AS cat_id,
SUBSTRING(UPPER(prd_key), 7, LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info;

--Checking for widespace in the prd_nm column
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--Checking for null or negative prices
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost <= 0 OR prd_cost IS NULL;

--Checking whether the product name with the Null 
SELECT prd_nm,
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_nm = 'HL Road Frame - Black- 58';

--Replacing a Null with a 0
SELECT  
ISNULL(prd_cost, 0) AS prd_cost
FROM bronze.crm_prd_info;

--Checking for low cardinality and normalize the data
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

--Normalization
SELECT  
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mounting'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line
FROM bronze.crm_prd_info;

--Checking for invalid date
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt < prd_cost;

--Using the Lead function to clean up the date
SELECT 
	prd_nm,
	prd_start_dt,
	LEAD(DATEADD(DAY, -1,prd_start_dt))  OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt
FROM bronze.crm_prd_info;

--====================================================
--Quality Checks for bronze.crm_sales_details
--====================================================
SELECT sls_ord_num
      ,sls_prd_key
      ,sls_cust_id
      ,sls_order_dt
      ,sls_ship_dt
      ,sls_due_dt
      ,sls_sales
      ,sls_quantity
      ,sls_price
  FROM bronze.crm_sales_details;


SELECT * FROM bronze.crm_sales_details;

--Checking for NULLS and widespaces in the data
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num IS NULL
OR sls_ord_num != TRIM(sls_ord_num);

--Checking whether the sls_prd_key column in the crm_sales_details is the same as the one in prd_key column from crm_prd_info
SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key IN (SELECT DISTINCT prd_key FROM bronze.crm_prd_info);

--Checking whether the sls_cust_id column has values in cst_id in silver.crm_cust_info
SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id IN (SELECT DISTINCT cst_id FROM silver.crm_cust_info);

--Converting data to Date

SELECT
CASE 
	WHEN LEN(sls_order_dt) != 8 OR sls_order_dt = 0 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE) 
END AS sls_order_dt	
FROM bronze.crm_sales_details

SELECT
CASE 
	WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt = 0 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE) 
END AS sls_ship_dt	
FROM bronze.crm_sales_details

SELECT
CASE 
	WHEN LEN(sls_due_dt) != 8 OR sls_due_dt = 0 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE) 
END AS sls_due_dt	
FROM bronze.crm_sales_details

--Checking whether the order_dt is less then the ship_dt
SELECT sls_order_dt 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt;

--Checking whether the ship date is less 
SELECT sls_ship_dt 
FROM bronze.crm_sales_details
WHERE sls_ship_dt > sls_due_dt;

--Checking for data consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative

SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_sales = 0 OR sls_sales < 0
OR sls_quantity IS NULL OR sls_quantity = 0 OR sls_quantity < 0
OR sls_price IS NULL OR sls_price = 0 OR sls_price < 0

--Creating a business
-->> If Sales is negative, zero, or null, derive it using Quantity and Price
-- >> If Price is zero or null calculate it using quantity and price
-- >> If Price is negative convert it to a positive value


SELECT DISTINCT
	sls_quantity,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
ELSE sls_sales
END AS sls_sales_test,

CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/NULLIF(sls_quantity,0)
     ELSE ABS(sls_price)
END AS sls_price_test
FROM bronze.crm_sales_details

--=========================================
--Quality Checks for bronze.erp_cust_az12
--=========================================

SELECT  
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
     ELSE cid
END AS cid
      ,bdate
      ,gen
FROM DataWarehouse.bronze.erp_cust_az12;

--Identify out of range dates
-->> checking for very old dates
SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01';

-->> checking for birthdates in the future
SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE();

--Removing birthdates in the future
SELECT 
CASE WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
END bdate
FROM bronze.erp_cust_az12;


--Data Normalisation and Consistency
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

SELECT 
CASE UPPER(TRIM(gen))
	WHEN 'F' THEN 'Female'
	WHEN 'Female' THEN 'Female'
	WHEN 'M' THEN 'Male'
	WHEN 'Male' THEN 'Male'
	ELSE NULL
END AS gen
FROM bronze.erp_cust_az12;

SELECT 
REPLACE(cid, '-', '') AS cid
FROM bronze.erp_loc_a101;

--Selecting * from bronze.erp_loc_a101
SELECT cid
      ,cntry
FROM DataWarehouse.bronze.erp_loc_a101;

SELECT 
CASE 
	WHEN UPPER(TRIM(cntry)) IN('DE' , 'Germany') THEN 'Germany'
	WHEN UPPER(TRIM(cntry)) IN('US', 'USA', 'United States')  THEN 'United States'
	WHEN TRIM(cntry) = ' '  OR TRIM(cntry) IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END	cntry
FROM bronze.erp_loc_a101;

SELECT * FROM bronze.erp_loc_a101;

--Selecting * from bronze.erp_loc_a101
SELECT id
      ,cat
      ,subcat
      ,maintenance
FROM bronze.erp_px_cat_g1v2;

--Checking for widespaces
SELECT cat
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat);

--Checking for Distinct values
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

--Checking for widespaces
SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat);

--Checking for Distinct values
SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;

--Checking for Distinct values
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;

--Checking for widespaces
SELECT maintenance
FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance);
