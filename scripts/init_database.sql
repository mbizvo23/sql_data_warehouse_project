/*

===============================================
Create Database and Schemas
===============================================
Script Purpose:
  This script creates the 'DataWarehouse' Database after checking if it already exists.
  If the database exxists it is dropped and recreated. Additionally the scipts sets up three schemas within the database,
  The 'bronze', 'silver', and 'gold'  Schemas.

Warning:
  Running this script will drop the entire 'DataWarehouse' database if it exists deleting all the data permanantely.
  Proceed with caution and ensure you have the proper backup before running the script.
*/


USE master;

--Drop and recreate the 'DataWarehouse' database
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
  BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
  END;
--Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO

--Create Schemas
CREATE SCHEMA bronze;

GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
