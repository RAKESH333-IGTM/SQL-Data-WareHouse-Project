USE [DataWarehouse]
GO

/****** Object:  StoredProcedure [bronze].[load_bronze]    Script Date: 30-06-2025 08:37:29 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-----LOADINGT THE DATA INTO THE TABLE-----------
CREATE   PROCEDURE [bronze].[load_bronze] AS ---Creating stored procedur is for the frequently used queries-----
BEGIN
    DECLARE @Start_time DATETIME, @End_time DATETIME 
    BEGIN TRY
		PRINT '=========================='
		PRINT 'Printing the Bronze Layer'
		PRINT '=========================='
		
		PRINT '-------------------------------------'
		SET @Start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info  --Truncate will helps us to refresh, when  the new data is loaded into the source data-----
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\DWH Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE()
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@Start_time,@End_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------'

		--SELECT * FROM bronze.crm_cust_info
		--SELECT COUNT(*) FROM bronze.crm_cust_info


		TRUNCATE TABLE bronze.crm_prd_info 
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\DWH Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'

		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		--SELECT * FROM bronze.crm_prd_info

		TRUNCATE TABLE bronze.crm_sales_details 
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\DWH Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		--SELECT * FROM bronze.crm_sales_details

		--============== --INGECTING THE ERP SOURCE DATA INTO THE TABLES--===========================--

		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\DWH Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
		   FIRSTROW = 2,
		   FIELDTERMINATOR = ',',
		   TABLOCK
		);
		--SELECT * FROM bronze.erp_cust_az12


		TRUNCATE TABLE bronze.erp_loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\DWH Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		--SELECT * FROM bronze.erp_loc_a101


		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\DWH Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		--SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2   ---This is to count the exact number of rows are injected or not into the tables------
	END TRY
	BEGIN CATCH  --It helps to monitor bottlenecks, optimize performance, monitor trends. detect issues--
	PRINT '========================================================'
	PRINT 'Error Messgae' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '========================================================'
	END CATCH
END
GO


