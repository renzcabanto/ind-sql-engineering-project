/* This will act as sketchpad for the Raw Data Exploration in prepartion for the transformation phase or "Silver" Layer of the database */

USE DataWarehouse;

Select top 1000 * 

	from  bronze.crm_cust_info; 

Select cst_key, 
			count(*)

	from bronze.crm_cust_info
	group by cst_key

	having count(*) > 1; /* has duplicates on the customer key */ 

Select cst_id, 
			count(*)

	from bronze.crm_cust_info
	group by cst_id

	having count(*) > 1; /* has duplicates on the customer id */ 

Select 
			cst_key,
			cst_firstname,
			cst_lastname
	from bronze.crm_cust_info
	where 
	cst_firstname != trim(cst_firstname)
	or cst_lastname != trim(cst_lastname); /* will need to trim the characters of the first name and last name */

Select 
			cst_marital_status,
			count(*) as count_of_records
	from bronze.crm_cust_info
	group by cst_marital_status; /* deletion of the null entries and possibly expounding the "S" and "M" */

Select 
			cst_gndr,
			count(*) as count_of_records
	from bronze.crm_cust_info
	group by cst_gndr; /* there is 25% null entries for the gender column so it will be beneficial if we put not available or 'n/a' instead */

/* Things needed to be cleaned in customer info 
-deduplication of cst_key
-deletion of white spaces on the first and lastnames 
-The marital status and gender are similar in values and can possibly cause confusion so it might be beneficial to create a longform label for them
- create a placeholder for the NUll values for the marital status and the gender

*/

Select top 1000 * 

	from  bronze.crm_prd_info; /* the product info doesn't match what is on the sales details table */ 

Select prd_key,
		count(*) as record_count

	from bronze.crm_prd_info
	group by prd_key

	having count(*) > 1; /* needs deduplication on prd_key level */ 


Select top 1000 * 

	from  bronze.crm_prd_info
	where 
	trim(prd_key) != prd_key; /* Clear on whitespaces in names */

Select top 1000 * 

	from  bronze.crm_prd_info
	where 
	prd_start_dt > prd_end_dt; /* there are certain dates where start date is greater than the end_date */

SELECT case when prd_start_dt > prd_end_dt then 'invalid' else 'valid' end as start_date_status,
		count(*)

	from  bronze.crm_prd_info

	group by case when prd_start_dt > prd_end_dt then 'invalid' else 'valid' end; /* Almost half of the products have invalid date dimensions? */

SELECT top 1000 * 

	from  bronze.crm_prd_info
	where 
	prd_cost is null or prd_cost < 0; /* product cost have null entries have null values, for this cases we'll just use zero to not cause null dropouts during computations */

SELECT prd_line,
		count(*) as record_count
	from bronze.crm_prd_info 
	group by prd_line; /* this can benefit from clearer product line definition, 'n/a' will also be used for the null product lines */


-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
Select top 1000 * 

from  bronze.crm_sales_details; 

