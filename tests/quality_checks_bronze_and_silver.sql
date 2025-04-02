/*
=================================================================
Quality Checks
=================================================================
Script purpose:
This script performs various quality checks for data consistency, accuracy, 
and standardisation accross the 'silver' schema. It includes checks for:
- Null or duplicate primary keys.
- Unwanted spaces in string fields. 
- Standardization and consistency. 
- Invalid date ranges and orders. 
- Data consistency between related fields. 

Usage notes:
- Investigate and solve discrepencies after running these checks. 
=================================================================
*/

--=================================================================
-- Checking bronze.crm_cust_info
--=================================================================
-- Check for Nulls or Duplicates in PK
-- Expectation: No result

SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*) >1 OR cst_id IS NULL

  -- check for unwanted spaces 
  
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

--Data standardization and consistancy

SELECT DISTINCT cst_grd
FROM bronze.crm_cust_info


SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

  --=================================================================
-- Checking silver.crm_cust_info
--=================================================================

-- Check for unwanted spaces
-- Expectation: No Results

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

--=================================================================
-- Checking bronze.crm_prd_info
--=================================================================

  -- check for nulls or negative numbers 
-- Expectation: No result
  
SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id 
HAVING COUNT(*) >1 OR prd_id IS NULL
-- no duplicates or nulls in this table


SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL

-- Check for unwanted spaces
-- Expectation: No Results

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm!= TRIM(prd_nm);

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- check for invalid date  orders
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt
--=================================================================
-- Checking silver.crm_prd_info
--=================================================================
SELECT COUNT(*) 
FROM silver.crm_prd_info

-- Lead accesses values from the next window within a row

SELECT *,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

--=================================================================
-- Checking bronze.crm_sales_details
--=================================================================

-- Check for Invalid Dates

-- Switch 0s to nulls
SELECT NULLIF(sls_order_dt, 0)
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0 OR LEN(sls_order_dt) !=8 OR sls_order_dt > 20500101 OR sls_order_dt<1900;

-- Switch 0s to nulls
SELECT NULLIF(sls_ship_dt, 0)
FROM bronze.crm_sales_details
WHERE sls_ship_dt <=0 OR LEN(sls_ship_dt) !=8 OR sls_ship_dt > 20500101 OR sls_ship_dt<1900;

-- check invalid orders
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_due_dt OR sls_order_dt > sls_ship_dt

SELECT 
sls_sales AS old_sls_sales,
sls_quantity,
sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity* ABS(sls_price) 
		THEN sls_quantity* ABS(sls_price) 
	 ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <=0 
	 THEN sls_sales/NULLIF(sls_quantity, 0) -- to stop division by zero error
	 END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL

--=================================================================
-- Checking bronze.erp_cust_az12
--=================================================================
--Identify out of range dates
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate <'1924-01-01' OR bdate > GETDATE()

-- low cardinality data check
SELECT DISTINCT gen,
CASE  WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	  WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	  ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

--=================================================================
-- Checking bronze.erp_loc_a101
--=================================================================
SELECT DISTINCT CNTRY
FROM bronze.erp_loc_a101
