/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates silver tables.
		- Inserts transformed and cleansed data from Bronze into silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '=====================================================';
		PRINT 'Cleaning and Loading data into ''silver.crm_cust_info';
		PRINT '=====================================================';

		SET @start_time = GETDATE()
		PRINT '>>Truncating the Table silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>> Inserting Data into silver.crm_cust_info';

		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) cst_firstname,
			TRIM(cst_lastname) cst_lastname,
			CASE WHEN cst_marital_status = UPPER(TRIM('S')) THEN 'Single'
				 WHEN cst_marital_status = UPPER(TRIM('M')) THEN 'Married'
				 ELSE 'n/a'
			END cst_marital_status,
			CASE WHEN cst_gndr = UPPER(TRIM('F')) THEN 'Female'
				 WHEN cst_gndr = UPPER(TRIM('M')) THEN 'Male'
				 ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM(
			SELECT 
			*,
			RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info) t
		WHERE flag_last = 1 AND cst_id IS NOT NULL;

		SET @end_time = GETDATE()
		PRINT 'Inserting data into silver.crm_cust_info table duration:' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' Seconds';
		Print '--------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '=====================================================';
		PRINT 'Cleaning and Loading data into silver.crm_prd_info';
		PRINT '=====================================================';

		PRINT '>>Truncating the Table silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT '>> Inserting Data into silver.crm_prd_info';

		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)


		SELECT 
			prd_id,
			UPPER(REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')) AS cat_id,
			SUBSTRING(UPPER(prd_key), 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mounting'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
			prd_start_dt,
			LEAD(DATEADD(DAY, -1,prd_start_dt))  OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt
		FROM bronze.crm_prd_info;

		SET @end_time = GETDATE()
		PRINT 'Inserting data into silver.crm_prd_info table duration:' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' Seconds';
		Print '--------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '========================================================';
		PRINT 'Cleaning and Loading data into silver.crm_sales_details';
		PRINT '========================================================';

		PRINT '>>Truncating the Table silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>> Inserting Data into silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price 
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN LEN(sls_order_dt) != 8 OR sls_order_dt = 0 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE) 
			END AS sls_order_dt,
			CASE 
				WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt = 0 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE) 
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales_test, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price_test
		FROM bronze.crm_sales_details


		SET @end_time = GETDATE()
		PRINT 'Inserting data into silver.crm_sales_details table duration:' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' Seconds';
		Print '--------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '=====================================================';
		PRINT 'Cleaning and Loading data into silver.erp_cust_az12';
		PRINT '=====================================================';

		PRINT '>>Truncating the Table silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT '>> Inserting Data into silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
			 cid
			,bdate
			,gen
		)

		SELECT  
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
			 ELSE cid
		END AS cid
			  ,CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END bdate
			  ,
		CASE UPPER(TRIM(gen))
			WHEN 'F' THEN 'Female'
			WHEN 'Female' THEN 'Female'
			WHEN 'M' THEN 'Male'
			WHEN 'Male' THEN 'Male'
			ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12;

		SET @end_time = GETDATE()
		PRINT 'Inserting data into silver.erp_cust_az12 table duration:' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' Seconds';
		Print '--------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '=====================================================';
		PRINT 'Cleaning and Loading data into silver.erp_loc_a101';
		PRINT '=====================================================';

		PRINT '>>Truncating the Table silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>> Inserting Data into silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
		cid, 
		cntry)

		SELECT 
		REPLACE(cid, '-', ' 
		') AS cid,
		CASE 
			WHEN UPPER(TRIM(cntry)) IN('DE' , 'Germany') THEN 'Germany'
			WHEN UPPER(TRIM(cntry)) IN('US', 'USA', 'United States')  THEN 'United States'
			WHEN TRIM(cntry) = ' '  OR TRIM(cntry) IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END	AS cntry
		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE()
		PRINT 'Inserting data into silver.erp_loc_a101 table duration:' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' Seconds';
		Print '--------------------------------------------';


		SET @start_time = GETDATE()
		PRINT '=====================================================';
		PRINT 'Cleaning and Loading data into silver.erp_px_cat_g1v2';
		PRINT '=====================================================';

		PRINT '>>Truncating the Table silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>> Inserting Data into silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
			   id
			  ,cat
			  ,subcat
			  ,maintenance
		)

		SELECT id
			  ,cat
			  ,subcat
			  ,maintenance
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE()
		PRINT 'Inserting data into silver.erp_loc_a101 table duration:' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' Seconds';
		Print '--------------------------------------------';

		SET @batch_end_time = GETDATE()
		PRINT 'Loading the silver layer duration:' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' Seconds';
		Print '--------------------------------------------';

	END TRY
	BEGIN CATCH
	PRINT'ERROR OCCURED WHILE LOADING THE SILVER LAYER';
	PRINT'ERROR MESSAGE:' + ERROR_MESSAGE();
	PRINT'ERROR NUMBER:' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT'ERROR LINE:' + CAST(ERROR_LINE() AS NVARCHAR);
	END CATCH
END;
