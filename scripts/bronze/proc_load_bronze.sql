/*
====================================================================
Stored Procedure: Load Bronze layer (source to Bronze)
====================================================================
Script Purpose:
*  The Stored procedure loads the data from external CSV file.
*  It first Truncatesd the tables from bronze layer before loading.
*  Uses the BULK INSERT command to insert data from CSV file into the bronze layer tasbles.
*  It calculates the during of laoding each table and of the overall procedure.
*  The stored procedure doesn't contain any Parameters.

Usage Example
Exec bronze.load_bronze;
*/

--Create Stored Procedure
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME
    BEGIN TRY
		SET @batch_start_time = GETDATE()

		Print '============================================';
		Print 'Insertimg Data from Source crm';
		Print '============================================';

		SET @start_time = GETDATE()
		--Truncating the Table bronze.crm_cust_info
		TRUNCATE TABLE bronze.crm_cust_info;

		--Inserting Data into bronze.crm_cust_info Table
		Print '>> Inserting Data into bronze.crm_cust_info Table';

		BULK INSERT bronze.crm_cust_info
		FROM 'D:\New folder\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE()
		PRINT 'Loading the bronze.crm_cust_info table duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		Print '--------------------------------------------';

		SET @start_time = GETDATE()
		--Truncating the Table bronze.crm_prd_info
		TRUNCATE TABLE bronze.crm_prd_info;

		--Inserting Data into bronze.crm_prd_info Table
		Print '>> Inserting Data into bronze.crm_prd_info Table';

		BULK INSERT bronze.crm_prd_info
		FROM 'D:\New folder\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE()
		PRINT 'Loading the bronze.crm_prd_info table duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		Print '--------------------------------------------';

		SET @start_time = GETDATE()
		--Truncating the Table bronze.crm_sales_details
		TRUNCATE TABLE bronze.crm_sales_details;

		--Inserting Data into bronze.crm_sales_details Table
		Print '>> Inserting Data into bronze.crm_sales_details Table';

		BULK INSERT bronze.crm_sales_details
		FROM 'D:\New folder\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
	SET @end_time = GETDATE()
		PRINT 'Loading the bronze.crm_sales_details table duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		Print '--------------------------------------------';

		Print '============================================';
		Print 'Insertimg Data from Source erp';
		Print '============================================';

		SET @start_time = GETDATE()
		--Truncating the Table bronze.erp_cust_az12
		TRUNCATE TABLE bronze.erp_cust_az12;

		--Inserting Data into bronze.erp_cust_az12 Table
		Print '>> Inserting Data into bronze.erp_cust_az12 Table';

		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\New folder\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
	SET @end_time = GETDATE()
		PRINT 'Loading the bronze.erp_cust_az12 table duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		Print '--------------------------------------------';

	    SET @start_time = GETDATE()
		--Truncating the Table bronze.erp_loc_a101
		TRUNCATE TABLE bronze.erp_loc_a101;

		--Inserting Data into bronze.erp_loc_a101 Table
		Print '>> Inserting Data into bronze.erp_loc_a101 Table';

		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\New folder\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE()
		PRINT 'Loading the bronze.erp_loc_a101 table: duration' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		Print '--------------------------------------------';
	
	    SET @start_time = GETDATE()
		--Truncating the Table bronze.erp_px_cat_g1v2
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		--Inserting Data into bronze.erp_px_cat_g1v2 Table
		Print '>> Inserting Data into bronze.erp_px_cat_g1v2 Table';

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\New folder\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE()
		PRINT 'Loading the bronze.erp_px_cat_g1v2 table duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		Print '--------------------------------------------';

	END TRY
	BEGIN CATCH
	 PRINT 'ERROR OCCURED WHILE LOADING BRONZE LAYER';
	 PRINT 'ERROR MESSAGE:'  + ERROR_MESSAGE();
	 PRINT 'ERROR NUMBER:' + CAST(ERROR_NUMBER() AS NVARCHAR);
	 PRINT 'ERROR LINE:' + CAST(ERROR_LINE() AS NVARCHAR);
	END CATCH

	SET @batch_end_time = GETDATE()
	PRINT 'Loading the bronze layer duration: ' + CAST(DATEDIFF(second,@batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
END

  
