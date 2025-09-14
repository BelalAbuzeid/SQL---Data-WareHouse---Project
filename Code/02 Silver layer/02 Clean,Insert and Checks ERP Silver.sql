/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

==============================
-- CLEANING DATA and CHECKS__
==============================

select 
*
from bronze.erp_cust_az12

select
cid,
case when cid like 'NAS%' then substring(cid,4,len(cid))				-- To remove (NAS) --
	 else cid
end cid
from bronze.erp_cust_az12

select 
bdate,
case when bdate > getdate () then Null									-- To make sure about Birthdate -- 
	 else bdate
end bdatee
from bronze.erp_cust_az12
order by bdatee

select distinct															-- To check full Name --
gen,
case trim(upper(gen))
	when 'F' then 'Female'
	when 'FEMALE' then 'Female'
	when 'M' then 'Male'
	when 'MALE' then 'Male'
	else 'N/A'
end gen
from bronze.erp_cust_az12




select
case when cid like 'NAS%' then substring(cid,4,len(cid))				
	 else cid
end cid,
case when bdate > getdate () then Null
	 else bdate
end bdate,
case trim(upper(gen))
	when 'F' then 'Female'
	when 'FEMALE' then 'Female'
	when 'M' then 'Male'
	when 'MALE' then 'Male'
	else 'N/A'
end gen
from bronze.erp_cust_az12

---------------------------------------------------------------------------------------------------
-- Inserting Clean Table to Silver Layer --

insert into silver.erp_cust_az12 (
			cid,
			bdate,
			gen )

select
case when cid like 'NAS%' then substring(cid,4,len(cid))				
	 else cid
end cid,
case when bdate > getdate () then Null
	 else bdate
end bdate,
case trim(upper(gen))
	when 'F' then 'Female'
	when 'FEMALE' then 'Female'
	when 'M' then 'Male'
	when 'MALE' then 'Male'
	else 'N/A'
end gen
from bronze.erp_cust_az12

-----------------------------------------------------------------------------------------------------
/* Then u need to check by different Queries to see if there any duplicated
values need to fix them before the next step by changing bronze with silver */


select distinct															-- To check full Name --
gen
from silver.erp_cust_az12

select
*
from silver.erp_cust_az12


-----------------------------------------------------------------------------------------------------
/* The same steps for next table */

select 
* 
from bronze.erp_loc_a101

select										-- to remove (-) from (cid) column -- 
cid,
replace(cid,'-','') as cid
from bronze.erp_loc_a101

select distinct								-- To Validate full name --
cntry,
case
	when trim(upper(cntry)) = 'DE' then 'Germany'
	when trim(upper(cntry)) in ('US','USA') then 'United States'
	when trim(upper(cntry)) is null or trim(upper(cntry)) = '' then 'N/A'
	else cntry
end cntry
from bronze.erp_loc_a101




select
replace(cid,'-','') as cid,
case
	when trim(upper(cntry)) = 'DE' then 'Germany'
	when trim(upper(cntry)) in ('US','USA') then 'United States'
	when trim(upper(cntry)) is null or trim(upper(cntry)) = '' then 'N/A'
	else cntry
end cntry
from bronze.erp_loc_a101

---------------------------------------------------------------------------------------------------
-- Inserting Clean Table to Silver Layer --

insert into silver.erp_loc_a101 (
			cid,
			cntry )

select
replace(cid,'-','') as cid,
case
	when trim(upper(cntry)) = 'DE' then 'Germany'
	when trim(upper(cntry)) in ('US','USA') then 'United States'
	when trim(upper(cntry)) is null or trim(upper(cntry)) = '' then 'N/A'
	else cntry
end cntry
from bronze.erp_loc_a101

-----------------------------------------------------------------------------------------------------
/* Then u need to check by different Queries to see if there any duplicated
values need to fix them before the next step by changing bronze with silver */

select
*
from silver.erp_loc_a101


select distinct
cntry
from silver.erp_loc_a101

-----------------------------------------------------------------------------------------------------
/* The same steps for next table */

select
*
from bronze.erp_px_cat_g1v2

select														-- Check for spaces --
*
from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2

---------------------------------------------------------------------------------------------------
-- Inserting Clean Table to Silver Layer --

insert into silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance )

select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2









