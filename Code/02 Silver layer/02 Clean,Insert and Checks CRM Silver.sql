=====================
-- CLEANING DATA __
=====================

/* we make sure that the primary key is unique so we used count(*)
or any value is null */

select 
cst_id,
count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

--------------------------------------------------------------------------------
/* then we used row number to rank the data desc and we took the newest data
to new column called flag */

select * ,
row_number() over(partition by cst_id order by cst_create_date desc) flag
from bronze.crm_cust_info
where cst_id = 29466


select
*
from(
	select 
	*,
	row_number() over(partition by cst_id order by cst_create_date desc) flag
	from bronze.crm_cust_info)t
where flag = 1
--------------------------------------------------------------------------------
/* then we checked if there any space in string values so we used trim func */

select cst_marital_status
from bronze.crm_cust_info
where cst_marital_status != trim(cst_marital_status)

select cst_gndr
from bronze.crm_cust_info
where cst_gndr != trim(cst_gndr)

select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)


select 
	cst_id,
	cst_key,
	trim(cst_firstname) cst_firstname,
	trim(cst_lastname) cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_Date,
	flag
from (
		select
			 *,
			 row_number() over(partition by cst_id order by cst_create_date desc) flag
		from bronze.crm_cust_info)t
where flag = 1
--------------------------------------------------------------------------------
/* then we replaced (F) and (M) from gender with full name 
and replaced (S) and (M) from Marital status ,
and used upper() to make sure that all data are capital */

select 
	cst_id,
	cst_key,
	trim(cst_firstname) cst_firstname,
	trim(cst_lastname) cst_lastname,
	case upper(trim(cst_marital_status))
		when 'S' then 'Single'
		when 'M' then 'Married'
		else 'N/A'
	end 
		cst_marital_status,
	case upper(trim(cst_gndr))
		when 'F' then 'Female'
		when 'M' then 'Male'
		else 'N/A'
	end 
		cst_gndr ,
	cst_create_Date,
	flag
from (
		select
			 *,
			 row_number() over(partition by cst_id order by cst_create_date desc) flag
		from bronze.crm_cust_info)t
where flag = 1
--------------------------------------------------------------------------------

-- Inserting Clean Table to Silver Layer --

insert into silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date )

select 
	cst_id,
	cst_key,
	trim(cst_firstname) cst_firstname,
	trim(cst_lastname) cst_lastname,
	case upper(trim(cst_marital_status))
		when 'S' then 'Single'
		when 'M' then 'Married'
		else 'N/A'
	end 
		cst_marital_status,
	case upper(trim(cst_gndr))
		when 'F' then 'Female'
		when 'M' then 'Male'
		else 'N/A'
	end 
		cst_gndr ,
	cst_create_Date
from (
		select
			 *,
			 row_number() over(partition by cst_id order by cst_create_date desc) flag
		from bronze.crm_cust_info)t
where flag = 1

--------------------------------------------------------------------------------
/* Then u need to check by different Queries to see if there any duplicated
PK or not to fix it before the next step by changing bronze with silver */

select 
cst_id,
count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null		 -- Output must be Empty --

select cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname)	 -- Output must be Empty --

select cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname)   -- Output must be Empty --

select distinct cst_gndr					 -- Output must be (Male,Female,N/A)
from silver.crm_cust_info

select distinct cst_marital_status			 -- Output must be (Single,Married,N/a)
from silver.crm_cust_info

-----------------------------------------------------------------------------------------------------
/* The same steps for next table */

select 
prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null       -- check duplicated values --

select									
prd_key
from bronze.crm_prd_info
where prd_key != trim(prd_key)				-- check about any space --

select								
prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)				-- check about any space --

select								
prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null		-- check about any -ve numbers or null --

select
distinct prd_line
from bronze.crm_prd_info					-- check cases to make full name --

select *
from bronze.crm_prd_info
where prd_start_dt > prd_end_dt				-- check if there any not logical date --

select
prd_key,
cast(prd_start_dt as date) prd_start_dt,
cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) -1 as date )prd_end_dte
from bronze.crm_prd_info
where prd_key = 'AC-HE-HL-U509-R'			-- Validate the end date to have concept and (-1) to select day before --


select 
*
from bronze.crm_prd_info

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

-----------------------------------------------------------------------------------------------------
-- Inserting Clean Table to Silver Layer --

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

-----------------------------------------------------------------------------------------------------
/* Then u need to check by different Queries to see if there any duplicated
PK or not to fix it before the next step by changing bronze with silver */

select 
prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null       -- check duplicated values --

select									
prd_key
from silver.crm_prd_info
where prd_key != trim(prd_key)				-- check about any space --

select								
prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm)				-- check about any space --

select								
prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null		-- check about any -ve numbers or null --

select
distinct prd_line
from silver.crm_prd_info					-- check cases to make full name --

select *
from silver.crm_prd_info
where prd_start_dt > prd_end_dt				-- check if there any not logical date --


select *
from silver.crm_prd_info

-----------------------------------------------------------------------------------------------------
/* The same steps for next table */

select *
from bronze.crm_sales_details


select sls_ord_num
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num)				-- check any spaces --


select sls_prd_key
from bronze.crm_sales_details
where sls_prd_key != trim(sls_prd_key)				-- check any spaces --


select
sls_ord_num,
nullif (sls_order_dt,0) sls_order_dt										-- Convert (0) to be Null --
from bronze.crm_sales_details
where sls_order_dt < 0 
	  or sls_order_dt = 0 or len(sls_order_dt) !=8							-- Make sure that 20021612 --

select
sls_ord_num,
sls_order_dt
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt											-- Make sure about Date Logic --


select distinct
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
	  or sls_sales is null or sls_quantity is null or sls_price is null
	  or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price


select distinct
sls_sales as oldsales,
sls_price as oldprice,
sls_quantity,
case when sls_price is null or sls_price <=0
		then sls_sales / nullif(sls_quantity,0)
	 else sls_price
end sls_price,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
		then sls_quantity * abs(sls_price)
	 else sls_sales
end sls_sales
from bronze.crm_sales_details




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

-----------------------------------------------------------------------------------------------------
-- Inserting Clean Table to Silver Layer --

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

-----------------------------------------------------------------------------------------------------
/* Then u need to check by different Queries to see if there any duplicated
PK or not to fix it before the next step by changing bronze with silver */

select *
from silver.crm_sales_details


select sls_ord_num
from silver.crm_sales_details
where sls_ord_num != trim(sls_ord_num)				-- check any spaces --


select sls_prd_key
from silver.crm_sales_details
where sls_prd_key != trim(sls_prd_key)				-- check any spaces --

select
sls_ord_num,
sls_order_dt
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt											-- Make sure about Date Logic --


select distinct
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
	  or sls_sales is null or sls_quantity is null or sls_price is null
	  or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price