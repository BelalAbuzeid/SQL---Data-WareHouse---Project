create view gold.dim_products as
select
	row_number() over(order by pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id product_id,
	pn.prd_key product_number,
	pn.prd_nm product_name,
	pn.cat_id category_id,
	pc.cat category,
	pc.subcat subcategory,
	pc.maintenance,
	pn.prd_cost cost,
	pn.prd_line product_line,  
	cast(pn.prd_start_dt as date) start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where pn.prd_end_dt is null					-- filter out historical data --\