-- Register and load the PiggyBank
REGISTER 'hdfs://localhost:9000/pig/piggybank.jar';
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load the CSV dataset
data = LOAD '/assignment/billionaires.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray, age:int);

-- Filter out headers
data = FILTER data by NOT rank is null;

-- Filter out rows with blank ages
data = FILTER data BY age IS NOT NULL;

-- Define age groups and assign each record to an age group
data_age_groups = FOREACH data GENERATE
    age,
    (CASE
        WHEN age >= 15 AND age <= 24 THEN '15-24'
        WHEN age >= 25 AND age <= 34 THEN '25-34'
        WHEN age >= 35 AND age <= 44 THEN '35-44'
        WHEN age >= 45 AND age <= 54 THEN '45-54'
        WHEN age >= 55 AND age <= 64 THEN '55-64'
        WHEN age >= 65 AND age <= 74 THEN '65-74'
        WHEN age >= 75 AND age <= 84 THEN '75-84'
        WHEN age >= 85 AND age <= 94 THEN '85-94'
        WHEN age >= 95 AND age <= 104 THEN '95-104'
        ELSE 'Unknown'
    END) AS age_groups;

-- Group by age group and count the number of records in each group
age_group_counts = GROUP data_age_groups BY age_groups;
age_group_counts_final = FOREACH age_group_counts GENERATE group AS age_group, COUNT(data_age_groups) AS count;

-- Store the results
STORE age_group_counts_final INTO '/output/age_group_counts';
