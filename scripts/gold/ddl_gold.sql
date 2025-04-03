/*
===========================================================
DDL Script: Create Gold Views
===========================================================
Script Purpose:
  This script creates views for the Gold Layer in the data warehouse.
  The Gold Layer represents the final dimension fact tables (star schema)

  Each view performs transformations and combines data from the Silver Layer
  to produce a clean, enriched, and business-ready dataset. 

  Usage:
    - These views can be queried directly for analysis and reporting. 
==============================================================
*/

CREATE VIEW gold.dim_customers AS 
	SELECT 
		ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		loc.cntry AS country,
		ci.cst_marital_status AS marital_status, 
		CASE WHEN ci.cst_grd != 'Unknown' THEN ci.cst_grd
			 WHEN cu.gen!= 'n/a' THEN COALESCE(cu.gen, 'unknown') --Coalesce chooses the first non null value in a list
			 ELSE 'Unknown'
		END AS gender,
		cu.bdate AS date_of_birth,
		ci.cst_create_date AS create_date;
	
		FROM silver.crm_cust_info ci
		LEFT JOIN silver.erp_cust_az12 AS cu
		ON ci.cst_key = cu.cid
		LEFT JOIN silver.erp_loc_a101 AS loc
		ON ci.cst_key = loc.cid


CREATE VIEW gold.dim_products AS
	SELECT ROW_NUMBER() OVER (ORDER BY p.prd_start_dt, p.prd_key) AS products,
		   p.prd_id AS product_id,	   
		   p.prd_key AS product_key,
		   p.prd_nm AS product_name,
		   p.prd_cat AS category_id,
		   cat.cat AS product_category ,
		   cat.cubcat AS subcategory,
		   cat.maintainance AS maintainance,
		   p.prd_cost AS cost,
		   p.prd_line AS product_line,
		   p.prd_start_dt AS start_date   
	       
	  FROM [DataWarehouse].[silver].[crm_prd_info] as p
	  LEFT JOIN silver.erp_px_cat_g1v2 as cat
	  ON p.prd_cat= cat.id
	  WHERE prd_end_dt IS NULL -- Filter out all historical data;

CREATE OR ALTER VIEW gold.fact_sales AS 
	SELECT 
		sls_ord_num AS order_number,
--Dimension keys
		pr.product_key,
		cu.customer_key,
--dates
       sls_order_dt AS order_date,
       sls_ship_dt as ship_date,
       sls_due_dt as due_date,
--mesurables
       sls_sales AS sales,
       sls_quantity AS quantity,
       sls_price AS price
	FROM [DataWarehouse].[silver].[crm_sales_details] AS sd
	LEFT JOIN gold.dim_products pr
	ON sd.sls_prd_key = pr.product_key
	LEFT JOIN gold.dim_customers cu
	ON sd.sls_cust_id = cu.customer_id;
      
  
