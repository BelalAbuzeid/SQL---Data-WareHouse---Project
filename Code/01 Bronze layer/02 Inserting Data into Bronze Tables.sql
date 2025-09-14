/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

-- Inserting Data into Tables --

create or alter procedure bronze.load_bronze_layer as
begin
    declare @start_time datetime , @end_time datetime , @start_time_batch date , @end_time_batch date
    begin try
        set @start_time_batch = GETDATE()
---------------------------------------------------------------------------------------------------------------
        set @start_time = GETDATE()
        print '>> Truncate and inserting into table: bronze.crm_cust_info'
        truncate table bronze.crm_cust_info

        bulk insert bronze.crm_cust_info
        from 'D:\Data Analysis\Data with Baraa\SQL Vedios Baraa\project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        with (
            fieldterminator = ',' ,
            rowterminator = '\n' ,
            firstrow = 2 ,
            tablock
            )
        set @end_time = GETDATE()
        print 'Loading time is: ' + cast(datediff(second,@start_time,@end_time) as varchar)
---------------------------------------------------------------------------------------------------------------
        set @start_time = GETDATE()
        print '>> Truncate and inserting into table: bronze.crm_prd_info'
        truncate table bronze.crm_prd_info

        bulk insert bronze.crm_prd_info
        from 'D:\Data Analysis\Data with Baraa\SQL Vedios Baraa\project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        with ( 
            fieldterminator = ',' ,
            rowterminator = '\n' ,
            firstrow = 2 ,
            tablock
            )
        set @end_time = GETDATE()
        print 'Loading time is: ' + cast(datediff(second,@start_time,@end_time) as varchar)
---------------------------------------------------------------------------------------------------------------
        set @start_time = GETDATE()
        print '>> Truncate and inserting into table: bronze.crm_sales_details'
        truncate table bronze.crm_sales_details

        bulk insert bronze.crm_sales_details
        from 'D:\Data Analysis\Data with Baraa\SQL Vedios Baraa\project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        with (
            fieldterminator = ',' ,
            rowterminator = '\n' ,
            firstrow = 2 ,
            tablock
            )
        set @end_time = GETDATE()
        print 'Loading time is: ' + cast(datediff(second,@start_time,@end_time) as varchar)
---------------------------------------------------------------------------------------------------------------
        set @start_time = GETDATE()
        print '>> Truncate and inserting into table: bronze.erp_loc_a101'
        truncate table bronze.erp_loc_a101

        bulk insert bronze.erp_loc_a101
        from 'D:\Data Analysis\Data with Baraa\SQL Vedios Baraa\project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        with (
            fieldterminator = ',' ,
            rowterminator = '\n' ,
            firstrow = 2 ,
            tablock
            )
        set @end_time = GETDATE()
        print 'Loading time is: ' + cast(datediff(second,@start_time,@end_time) as varchar)
---------------------------------------------------------------------------------------------------------------
        set @start_time = GETDATE()
        print '>> Truncate and inserting into table: bronze.erp_cust_az12'
        truncate table bronze.erp_cust_az12

        bulk insert bronze.erp_cust_az12
        from 'D:\Data Analysis\Data with Baraa\SQL Vedios Baraa\project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        with (
            fieldterminator = ',' ,
            rowterminator = '\n' ,
            firstrow = 2 ,
            tablock
            )
        set @end_time = GETDATE()
        print 'Loading time is: ' + cast(datediff(second,@start_time,@end_time) as varchar)
---------------------------------------------------------------------------------------------------------------
        set @start_time = GETDATE()
        print '>> Truncate and inserting into table: bronze.erp_px_cat_g1v2'
        truncate table bronze.erp_px_cat_g1v2

        bulk insert bronze.erp_px_cat_g1v2
        from 'D:\Data Analysis\Data with Baraa\SQL Vedios Baraa\project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        with (
            fieldterminator = ',' ,
            rowterminator = '\n' ,
            firstrow = 2 ,
            tablock
            )
        set @end_time = GETDATE()
        print 'Loading time is: ' + cast(datediff(second,@start_time,@end_time) as varchar)
        print '-----------------'
        
---------------------------------------------------------------------------------------------------------------
        set @end_time_batch = GETDATE()
        print 'Loading time whole batch is: ' + cast(datediff(second,@start_time_batch,@end_time_batch) as varchar)
    end try
    begin catch
        print '==========================================================='
        print ' Error Occured'
        print ' Error Message: ' + error_message()
        print ' Error Number: ' + cast(error_number() as varchar)
        print '==========================================================='
    end catch
end


go


exec bronze.load_bronze_layer
