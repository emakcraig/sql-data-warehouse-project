/*
=====================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=====================================================================
Script Purpose:
  The stored procedure loads data in to the bronze schema from external CSV files.
  It:
  - Truncates the bronze tables 
  - Uses the 'Bulk Insert' command to load data from csv files

Parameters:
  None.
This stored procedure does not accept any parameters or return any values

Usage Example: 
  EXEC bronze.load_bronze;
======================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	BEGIN TRY
	DECLARE @start_time DATETIME, @end_time DATETIME;

		PRINT '===============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===============================================';

		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;
		-- This clears the table. Must be done otherwise table could be filled twice.
		PRINT '>> Inserting Data Into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\EmmaC\OneDrive\Documents\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>--------------';


		--SELECT COUNT(*) FROM bronze.crm_cust_info;
		-- Has one less row than file due to exculding the header
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;
		-- This clears the table. Must be done otherwise table could be filled twice.
		PRINT '>> Inserting Data Into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\EmmaC\OneDrive\Documents\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>--------------';

		
		PRINT '>> Truncating Table: bronze.crm_sales_details'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		-- This clears the table. Must be done otherwise table could be filled twice.
		PRINT '>> Inserting Data Into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\EmmaC\OneDrive\Documents\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>--------------';


		PRINT '-----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------';

		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		-- This clears the table. Must be done otherwise table could be filled twice.
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\EmmaC\OneDrive\Documents\sql\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>--------------';


		PRINT '>> Truncating Table: bronze.erp_loc_A101';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_A101;
		-- This clears the table. Must be done otherwise table could be filled twice.
		PRINT '>> Inserting Data Into: bronze.erp_loc_A101';
		BULK INSERT bronze.erp_loc_A101
		FROM 'C:\Users\EmmaC\OneDrive\Documents\sql\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>--------------';


		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		-- This clears the table. Must be done otherwise table could be filled twice.
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\EmmaC\OneDrive\Documents\sql\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>--------------';


	END TRY
	-- If try block fails catch block handles the error
	BEGIN CATCH 
		PRINT '========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE ' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT '========================================='
	END CATCH
END
