DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load the CSV dataset
data = LOAD '/big_data_management/billionaire.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray, age:int, country:chararray, city:chararray, source:chararray,industries:chararray, countryOfCitizenship:chararray, organization:chararray, selfMade:chararray, status:chararray, gender:chararray, birthDate:chararray, lastName:chararray, firstName:chararray, title:chararray, date:chararray, state:chararray, residenceStateRegion:chararray, birthYear:int, birthMonth:int, birthDay:int, cpi_country:bigdecimal);

-- Filter out headers
data = FILTER data by NOT rank is null;

-- Filter out rows with blank cpi_country
data = FILTER data BY cpi_country IS NOT NULL;

-- Filter out rows with blank country
data = FILTER data BY country IS NOT NULL;

-- Group the countries
grouped_countries = GROUP data BY country;

values = FOREACH grouped_countries GENERATE group AS country, SUM(data.finalWorth) AS total_worth, AVG(data.cpi_country) as cpi_country;

STORE values INTO '/output/country_worth_cpi' USING PigStorage(',');

