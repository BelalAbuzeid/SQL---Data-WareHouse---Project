/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

===========================
-- Inserting ALL Tables --
===========================
create or alter procedure silver.load_silver_layer as
begin
	begin try

		truncate table silver.erp_cust_az12
		print '>> Inserting into silver.erp_cust_az12'

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

		----------------------------------------------------------------------------------------------

		truncate table silver.crm_prd_info
		print '>> Inserting into silver.crm_prd_info'

		insert into silver.crm_prd_info (
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt )

		select
		prd_id,
		replace(substring(trim(prd_key),0,6),'-','_') cat_id,
		substring(trim(prd_key),7,len(trim(prd_key))) prd_key,
		prd_nm,
		coalesce(prd_cost,0) prd_cost,
		case upper(trim(prd_line))
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'T' then 'Touring'
			else 'N/A'
		end prd_line,
		cast(prd_start_dt as date) prd_start_dt,
		cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) -1 as date )prd_end_dte
		from bronze.crm_prd_info

		----------------------------------------------------------------------------------------------

		truncate table silver.crm_sales_details
		print '>> Inserting into silver.crm_sales_details'

		insert into silver.crm_sales_details (
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

		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case
			when len(sls_order_dt) != 8 then Null
			when sls_order_dt = 0 then Null
			else cast(cast(sls_order_dt as varchar) as date)
		end sls_order_dt,
		case
			when len(sls_ship_dt) != 8 then Null
			when sls_ship_dt = 0 then Null
			else cast(cast(sls_ship_dt as varchar) as date)
		end sls_ship_dt,
		case
			when len(sls_due_dt) != 8 then Null
			when sls_due_dt = 0 then Null
			else cast(cast(sls_due_dt as varchar) as date)
		end sls_due_dt,
		case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
				then sls_quantity * abs(sls_price)
			 else sls_sales
		end sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <=0
				then sls_sales / nullif(sls_quantity,0)
			 else sls_price
		end sls_price
		from bronze.crm_sales_details

		----------------------------------------------------------------------------------------------

		truncate table silver.erp_cust_az12
		print '>> Inserting into silver.erp_cust_az12'

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

		----------------------------------------------------------------------------------------------

		truncate table silver.erp_loc_a101
		print '>> Inserting into silver.erp_loc_a101'

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

		----------------------------------------------------------------------------------------------

		truncate table silver.erp_px_cat_g1v2
		print '>> Inserting into silver.erp_px_cat_g1v2'

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
	end try
	begin catch
		print '==========================================================='
        print ' Error Occured'
        print ' Error Message: ' + error_message()
        print ' Error Number: ' + cast(error_number() as varchar)
        print '==========================================================='
	end catch
end

exec silver.load_silver


