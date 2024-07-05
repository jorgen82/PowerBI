-- Register and load the PiggyBank
REGISTER 'hdfs://localhost:9000/pig/piggybank.jar';
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load the CSV file, skipping the first line (header)
data = LOAD 'hdfs://localhost:9000/assignment/billionaires.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray, age:int, country:chararray);
data = FILTER data by NOT rank is null;
data_replaced = FOREACH data GENERATE (country is null or country == '' ? 'UNKNOWN' : country) as country;

-- Count the grouped data 
counts_grouped = GROUP data_replaced BY country;
counts = FOREACH counts_grouped GENERATE group AS country, COUNT(data_replaced) AS count;
STORE counts INTO '/output/country_counts' USING PigStorage(',');
