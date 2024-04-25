-- Register and load the PiggyBank
REGISTER '/home/hadoop/pig/piggybank.jar';
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load the CSV file, skipping the first line (header)
data = LOAD '/assignment/billionaires.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray, age:int);
data = FILTER data by NOT rank is null;
data_replaced = FOREACH data GENERATE (age is null ? -999 : age) as age;

-- Count the grouped data 
counts = FOREACH (GROUP data_replaced BY age) GENERATE group AS age, COUNT(data_replaced) AS age;
STORE counts INTO '/output/age_counts' USING PigStorage(',');

