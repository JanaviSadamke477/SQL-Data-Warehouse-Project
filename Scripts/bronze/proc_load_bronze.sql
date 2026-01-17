/*
==============================================================================================
Stored procedure: Load Bronze Layer(Source -> Bronze)
===============================================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
     - Truncates the bronze tables before loading data.
     - uses the 'BULK INSERT' command to load data from CSV files to Bronze tables.

Parameters: None
            This stored procedure does not accept any parametrs or return any values.

Usage Example:
               EXEC bronze.load_bronze;
===============================================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
            SET @batch_start_time = GETDATE();
            PRINT'=================================================';
            PRINT'Loading Bronze Layer';
            PRINT'=================================================';


            PRINT'-------------------------------------------------';
            PRINT'Loading CRM Tables';
            PRINT'-------------------------------------------------';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.crm_cust_info';
            truncate table bronze.crm_cust_info;

            PRINT'>> Inserting Data Into: bronze.crm_cust_info';
            BULK INSERT bronze.crm_cust_info
            FROM 'C:\Users\user\Downloads\source_crm\cust_info.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + 'seconds';
            print '>> -------------';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.crm_prd_info';
            truncate table bronze.crm_prd_info;

            PRINT'>> Inserting Data Into: bronze.crm_prd_info';
            BULK INSERT bronze.crm_prd_info
            FROM 'C:\Users\user\Downloads\source_crm\prd_info.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + 'seconds';
            print '>> -------------';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.crm_sales_details';
            truncate table bronze.crm_sales_details;

            PRINT'>> Inserting Data Into: bronze.crm_sales_details';
            BULK INSERT bronze.crm_sales_details
            FROM 'C:\Users\user\Downloads\source_crm\sales_details.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + 'seconds';
            print '>> -------------';

            PRINT'-------------------------------------------------';
            PRINT'Loading ERP Tables';
            PRINT'-------------------------------------------------';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.erp_cust_az12';
            truncate table bronze.erp_cust_az12;

            PRINT'>> Inserting Data Into: bronze.erp_cust_az12';
            BULK INSERT bronze.erp_cust_az12
            FROM 'C:\Users\user\Downloads\souce_erp\cust_az12.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + 'seconds';
            print '>> -------------';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.erp_loc_a101';
            truncate table bronze.erp_loc_a101;

            PRINT'>> Inserting Data Into: bronze.erp_loc_a101';
            BULK INSERT bronze.erp_loc_a101
            FROM 'C:\Users\user\Downloads\souce_erp\loc_a101.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + 'seconds';
            print '>> -------------';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
            truncate table bronze.erp_px_cat_g1v2;

            PRINT'>> Inserting Data Into: bronze.erp_px_cat_g1v2';
            BULK INSERT bronze.erp_px_cat_g1v2
            FROM 'C:\Users\user\Downloads\souce_erp\px_cat_g1v2.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + 'seconds';
            print '>> -------------';

            SET @batch_end_time = GETDATE();
            PRINT '================================================'
            PRINT ' Loading Bronze Layer is Completed';
            PRINT '>>  Toatl Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
            PRINT '================================================'
        END TRY
        BEGIN CATCH
            PRINT '================================================'
            PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
            PRINT 'Error Message' + ERROR_MESSAGE();
            PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
            PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
            PRINT '================================================'
        END CATCH
END

