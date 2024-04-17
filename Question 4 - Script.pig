DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load the CSV dataset
data = LOAD '/big_data_management/billionaire.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray);

-- Filter out headers
data = FILTER data by NOT rank is null;

-- Group by category
grouped_data = GROUP data BY category;

-- Calculate the count of people in each category
category_counts = FOREACH grouped_data GENERATE group AS category, COUNT(data) AS count_people;

-- Calculate the total count of people
total_count = SUM(category_counts.count_people);

-- Calculate the percentage of each category count over the total count
category_pct = FOREACH category_counts GENERATE category, count_people, (count_people / (double)total_count) * 100 AS pct;

-- Calculate the average worth of people in each category
category_worth_avg = FOREACH grouped_data GENERATE group AS category, AVG(data.worth) AS avg_worth;

-- Find the person with the maximum worth in each category
category_max_worth = FOREACH grouped_data {
    max_person_worth = MAX(data.worth);
    max_person_details = FILTER data BY worth == max_person_worth;
    GENERATE
        group AS category,
        FLATTEN(max_person_details) AS (name:chararray, worth:double);
}

-- Merge the results into one relation
merged_data = JOIN category_counts BY category, category_percentages BY category, category_worth_avg BY category, category_max_worth BY category;

-- Store the results
STORE merged_data INTO '/output/country_worth_cpi' USING PigStorage(',');

