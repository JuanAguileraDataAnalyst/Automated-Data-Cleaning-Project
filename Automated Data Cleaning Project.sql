/* PROJECT DETAILS
Automated Data Cleaning for US Household Income Data

This project involves creating an automated data cleaning process for the US Household Income dataset. 
The primary objective is to ensure data quality and consistency by removing duplicates, correcting typos, and standardizing text fields. 
The process is implemented using SQL stored procedures, events, and triggers, ensuring that the data cleaning steps are executed 
automatically after each data insertion and periodically every 30 days.

Key features of the project include:

1. Data Copy and Transformation: Data is copied from the original table to a new cleaned table with an additional timestamp for tracking.
2. Duplicate Removal: Automated identification and deletion of duplicate rows based on unique identifiers and timestamps.
3. Data Standardization: Correction of typographical errors and conversion of text fields to uppercase for uniformity.
4. Scheduled Data Cleaning: Implementation of an SQL event to run the cleaning procedure at regular intervals.
5. Real-time Data Cleaning: Use of SQL triggers to ensure that new data is cleaned immediately upon insertion.
6. Debugging and Validation: SQL queries are provided to verify the effectiveness of the cleaning process by checking for remaining duplicates 
and summarizing data counts.

This project enhances data reliability, making it suitable for further analysis and reporting, ensuring accurate and actionable insights from 
the US Household Income dataset.
*/
-- Displaying the original and cleaned data
SELECT * 
FROM us_household_income.us_household_income;

SELECT * 
FROM us_household_income.us_household_income_cleaned;

-- Setting up a delimiter for procedure creation
DELIMITER $$

-- Dropping the procedure if it already exists to avoid duplication errors
DROP PROCEDURE IF EXISTS Copy_and_clean_Data;

-- Creating a stored procedure to copy and clean data
CREATE PROCEDURE Copy_and_clean_Data()
BEGIN

    -- Creating the cleaned table if it doesn't already exist
    CREATE TABLE IF NOT EXISTS `us_household_income_Cleaned` (
      `row_id` int DEFAULT NULL,
      `id` int DEFAULT NULL,
      `State_Code` int DEFAULT NULL,
      `State_Name` text,
      `State_ab` text,
      `County` text,
      `City` text,
      `Place` text,
      `Type` text,
      `Primary` text,
      `Zip_Code` int DEFAULT NULL,
      `Area_Code` int DEFAULT NULL,
      `ALand` int DEFAULT NULL,
      `AWater` int DEFAULT NULL,
      `Lat` double DEFAULT NULL,
      `Lon` double DEFAULT NULL,
      `TimeStamp` TIMESTAMP DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

    -- Copying data from the original table to the cleaned table with a current timestamp
    INSERT INTO `us_household_income_Cleaned`
    SELECT * , CURRENT_TIMESTAMP
    FROM us_household_income.us_household_income;

    -- Data Cleaning Steps

    -- 1. Remove Duplicates
    -- Deleting duplicate rows based on 'id' and 'TimeStamp' fields
    DELETE FROM us_household_income_cleaned 
    WHERE 
        row_id IN (
        SELECT row_id
        FROM (
            SELECT row_id, id,
                ROW_NUMBER() OVER (
                    PARTITION BY id, `TimeStamp`
                    ORDER BY id, `TimeStamp`) AS row_num
            FROM 
                us_household_income_cleaned
        ) duplicates
        WHERE 
            row_num > 1
    );

    -- 2. Fixing some data quality issues by correcting typos and standardizing text fields
    UPDATE us_household_income_cleaned
    SET State_Name = 'Georgia'
    WHERE State_Name = 'georia';

    -- Converting 'County' values to uppercase for consistency
    UPDATE us_household_income_cleaned
    SET County = UPPER(County);

    -- Converting 'City' values to uppercase for consistency
    UPDATE us_household_income_cleaned
    SET City = UPPER(City);

    -- Converting 'Place' values to uppercase for consistency
    UPDATE us_household_income_cleaned
    SET Place = UPPER(Place);

    -- Converting 'State_Name' values to uppercase for consistency
    UPDATE us_household_income_cleaned
    SET State_Name = UPPER(State_Name);

    -- Correcting a typo in the 'Type' field
    UPDATE us_household_income_cleaned
    SET `Type` = 'CDP'
    WHERE `Type` = 'CPD';

    -- Standardizing 'Type' field values
    UPDATE us_household_income_cleaned
    SET `Type` = 'Borough'
    WHERE `Type` = 'Boroughs';

END $$
DELIMITER ;

-- Calling the stored procedure to clean data
CALL Copy_and_clean_Data();

-- Creating an event to run the data cleaning procedure every 30 days
DROP EVENT IF EXISTS run_data_cleaning;
CREATE EVENT run_data_cleaning
    ON SCHEDULE EVERY 30 DAY
    DO CALL Copy_and_clean_Data();

-- Creating a trigger to run the data cleaning procedure after each insert on the original table
DELIMITER $$
CREATE TRIGGER Transfer_clean_data
    AFTER INSERT ON us_household_income.us_household_income
    FOR EACH ROW 
BEGIN
    CALL Copy_and_clean_Data();
END $$
DELIMITER ;

-- Debugging or checking the stored procedure's results

-- Checking for remaining duplicates in the cleaned data
SELECT row_id, id, row_num
FROM (
    SELECT row_id, id,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY id) AS row_num
    FROM 
        us_household_income_cleaned
) duplicates
WHERE 
    row_num > 1;

-- Counting the number of rows in the cleaned table
SELECT COUNT(row_id)
FROM us_household_income_cleaned;

-- Counting occurrences of each state name in the cleaned table
SELECT State_Name, COUNT(State_Name)
FROM us_household_income_cleaned
GROUP BY State_Name;
