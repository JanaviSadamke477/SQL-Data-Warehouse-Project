/* 
===============================================================
Create Database and Schemas
===============================================================
Script purpose:
    This script creates a ne database named 'DataWarehouse' after checking if it already exists.
    If the database exists, it is droppedand recreated.Addionally the script sets up three schemas 
    within the datbases: 'bronze', 'silver', and 'gold'.

WARNING:
  Running this script will drop the entire 'DataWarehouse' databasse if it exists.
  All the data in the database will be permanently deleted. proceed with caution
  and ensure you have proper backups before running this script.
*/

USE master;
Go

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO
-- Creating data warehouse
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO

-- Creating schemas
CREATE SCHEMA Bronze;
Go
CREATE SCHEMA Silver;
Go
CREATE SCHEMA Gold;
