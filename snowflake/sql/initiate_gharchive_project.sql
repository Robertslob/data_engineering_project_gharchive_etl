-- Version: 1.1
-- Author: Robert Slob

USE ROLE accountadmin;

-- Create the main container
CREATE OR REPLACE DATABASE gharchive_project;

-- Create a schemas following the medaillon structure
CREATE OR REPLACE SCHEMA gharchive_project.staging;
CREATE OR REPLACE SCHEMA gharchive_project.core;
CREATE OR REPLACE SCHEMA gharchive_project.marts;
CREATE OR REPLACE SCHEMA gharchive_project.sandbox;

CREATE OR REPLACE STAGE gharchive_project.staging.s3_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://amzn-s3-gharchive-processed-rslob/'
  FILE_FORMAT = (TYPE = 'PARQUET');