/*

Create The Database and Schemas

Script Purpose
  This script creates a new database after checking if it already exists. 
  It also sets it up with three schemas withing the database. 

WARNING:
Running this script will DELETE the datawarehouse if it already exists. 
/*

-- Create Database 'DataWarehouse'

-- Master is used to create databases 
USE master;
GO

-- Drop and recreate database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;

-- Create Schemas
-- The Go key word is needed when one command must be completed completley before the next. 
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
