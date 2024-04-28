-- Register and load the PiggyBank
REGISTER 'hdfs://localhost:9000/pig/piggybank.jar';
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load the CSV dataset
data = LOAD 'hdfs://localhost:9000/assignment/billionaires.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray, age:int, country:chararray, city:chararray, source:chararray,industries:chararray, countryOfCitizenship:chararray, organization:chararray, selfMade:chararray, status:chararray, gender:chararray, birthDate:chararray, lastName:chararray, firstName:chararray, title:chararray, date:chararray, state:chararray, residenceStateRegion:chararray, birthYear:int, birthMonth:int, birthDay:int, cpi_country:float);

-- Filter out headers
data = FILTER data by NOT rank is null;

-- Filter out rows with blank country
data = FILTER data BY NOT country IS NULL AND NOT (country MATCHES '');

-- Replace empty cpi_country with -999
data = FOREACH data GENERATE country, finalWorth, (cpi_country is null ? -999 : cpi_country) as cpi_country;

-- Group the countries
grouped_countries = GROUP data BY country;

values = FOREACH grouped_countries GENERATE group AS country, AVG(data.cpi_country) as cpi_country, SUM(data.finalWorth) AS total_worth;

STORE values INTO '/output/country_worth_cpi' USING PigStorage(',');

