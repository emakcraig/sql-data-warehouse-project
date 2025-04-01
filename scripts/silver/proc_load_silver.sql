/*
===================================================================================================
DDL Script: Create Silver Tables 
===================================================================================================
Script Purpose:
  This script creates tables in the 'silver' schema, dropping existing tables if they already exist. 
  Running this script re-defines the DDL structure of 'bronze tables
===================================================================================================
*/

EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 

	BEGIN TRY
	DECLARE @start_time DATETIME, @end_time DATETIME;

		PRINT '===============================================';
		PRINT 'Loading Silver Layer';
		PRINT '===============================================';

		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------';
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_cust_info'
	TRUNCATE TABLE silver.crm_cust_info
	PRINT '>> Inserting Data Into: silver.crm_cust_info'	

	INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_grd,
	cst_create_date)

	SELECT
	cst_id,
	cst_key, 
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_grd)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_grd)) = 'M' THEN 'Male'
		ELSE 'Unknown'
	END cst_grd,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'Unknown'
	END cst_marital_status,
	cst_create_date
	FROM(
	SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info 
	WHERE cst_id IS NOT NULL
	)t WHERE flag_last =1;	
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>>--------------';

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_prd_info'
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting Data Into: silver.crm_prd_info'
	INSERT INTO silver.crm_prd_info(
		prd_id,
		prd_cat,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt

	)	

	  SELECT prd_id,
      
		  REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') AS cat_id, -- Derived new category column
		  SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract product key
		  prd_nm,
		  ISNULL (prd_cost, 0) AS prd_cost, -- managing nulls
		  -- replace null with 0
		  CASE UPPER(TRIM(prd_line))
			   WHEN 'M' THEN 'Mountain'
			   WHEN 'R' THEN 'Road'
			   WHEN 'T' THEN 'Touring'
			   WHEN 'S' THEN 'Other'
			   ELSE 'n/a'
		  END AS prd_line, -- Data normalization. Map product line codes to descriptive values. 
		  CAST (prd_start_dt AS DATE) AS prd_start_dt, -- Converted data type
		  CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- Data enrichment. Added correct data on order date. 
	FROM bronze.crm_prd_info
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>>--------------';

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_sales_details'
	TRUNCATE TABLE silver.crm_sales_details;			
	PRINT '>> Inserting Data Into: silver.crm_sales_details'

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

	SELECT sls_ord_num,
		  sls_prd_key,
		  sls_cust_id,
		  CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			   ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- Need to convert to varchar before converting to date
		  END AS sls_order_dt,
      
		  CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			   ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			   END AS sls_ship_dt,
      
		  CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			   ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			   END AS sls_due_dt,     
        
		  CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity* ABS(sls_price) 
			   THEN sls_quantity* ABS(sls_price) 
			   ELSE sls_sales
		  END AS sls_sales,
		  sls_quantity,	
		  CASE WHEN sls_price IS NULL OR sls_price <=0 
			   THEN sls_sales/NULLIF(sls_quantity, 0) -- to stop division by zero error
			   ELSE sls_price
		  END AS sls_price
   
	  FROM bronze.crm_sales_details;
	  PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>>--------------';

		PRINT '-----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------';

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_cust_az12';
			
	TRUNCATE TABLE silver.erp_cust_az12;
			
	PRINT '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12(
	cid ,
	bdate,
	gen
	)

	SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
			END AS cid,
	CASE WHEN bdate > GETDATE()
		 THEN NULL 
		 ELSE bdate
		 END AS bdate,
	CASE  WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		  WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		  ELSE 'n/a'
	END AS gen

	FROM [DataWarehouse].[bronze].[erp_cust_az12];

	PRINT '>> Truncating Table: silver.erp_loc_a101';
			TRUNCATE TABLE silver.erp_loc_a101;
			PRINT '>> Inserting Data Into: silver.erp_loc_a101';
			INSERT INTO silver.erp_loc_a101 (
				cid,
				cntry
			)
			SELECT
				REPLACE(cid, '-', '') AS cid, 
				CASE
					WHEN TRIM(cntry) = 'DE' THEN 'Germany'
					WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
					WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
					ELSE TRIM(cntry)
				END AS cntry -- Normalize and Handle missing or blank country codes
			FROM bronze.erp_loc_a101;
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>>--------------';
	
	
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
			
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
			
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	cubcat,
	maintainance
	)
	SELECT id,
	cat,
	cubcat,
	maintainance
	FROM bronze.erp_px_cat_g1v2;
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>>--------------';
	END TRY

	BEGIN CATCH 
		PRINT '========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'ERROR MESSAGE ' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT '========================================='
	END CATCH
END
