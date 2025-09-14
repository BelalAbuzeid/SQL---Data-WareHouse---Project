select count(*) from (	                 -- To check Duplicates --
	select 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca
	on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 la
	on ci.cst_key = la.cid
	)t
group by cst_id
having count(*) >1


select															-- To Check Integration due to Joins --
		ci.cst_gndr,
		ca.gen,
		case when ci.cst_gndr != 'N/A' then ci.cst_gndr
		else coalesce (ca.gen, 'N/A')
		end New_Gen
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca
	on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 la
	on ci.cst_key = la.cid



	   

create view gold.dim_customers as 
select 
		row_number() over(order by cst_id) as customer_key,
		ci.cst_id customer_id,
		ci.cst_key customer_number,
		ci.cst_firstname first_name,
		ci.cst_lastname last_name,
		la.cntry country,
		ci.cst_marital_status marital_status,
		case when ci.cst_gndr != 'N/A' then ci.cst_gndr
		else coalesce (ca.gen, 'N/A')
		end gender,
		ca.bdate birthdate,
		ci.cst_create_date create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid 