-- Register and load the PiggyBank
REGISTER '/home/hadoop/pig/piggybank.jar';
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load the CSV dataset
data = LOAD '/assignment/billionaires.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray);

-- Filter out headers
data = FILTER data by NOT rank is null;

-- Filter out nulls
data = FILTER data by NOT finalWorth is null;
data = FILTER data by NOT category is null;
data = FILTER data by NOT personName is null;

-- Group by category
grouped_data = GROUP data BY category;

-- Calculate the count of people in each category
category_counts = FOREACH grouped_data GENERATE group AS category, COUNT(data) AS count_people;

-- Calculate the total count of people
total_count = FOREACH (GROUP category_counts ALL) GENERATE SUM(category_counts.count_people) AS total_count;

-- Calculate the percentage of each category count over the total count
category_pct = FOREACH category_counts GENERATE category, count_people, ((double)count_people / (double)total_count.total_count) * 100 AS pct;

-- Calculate the average worth of people in each category
category_worth_avg = FOREACH grouped_data GENERATE group AS category, AVG(data.finalWorth) AS avg_worth;

-- Find the person with the maximum worth in each category
category_max_worth = FOREACH grouped_data {
    max_person_worth = MAX(data.finalWorth);
    max_person_details = FILTER data BY worth == max_person_worth;
    GENERATE
        group AS category,
        FLATTEN(max_person_details) AS (name:chararray, worth:double);
}


category_max_worth = FOREACH grouped_data {
    sorted_data = ORDER data BY finalWorth DESC;
    top_records = LIMIT sorted_data 1;
    GENERATE
        group AS category,
        FLATTEN(top_records.personName) AS max_finalWorth_personName;
}


-- Merge the results into one relation
merged_data = JOIN category_counts BY category, category_percentages BY category, category_worth_avg BY category, category_max_worth BY category;

-- Store the results
STORE merged_data INTO '/output/country_worth_cpi' USING PigStorage(',');





max_finalWorth_in_group = FOREACH grouped_data GENERATE group AS category, MAX(data_filtered.finalWorth) AS max_finalWorth;

-- Find the 'personName' with the maximum 'finalWorth' in each category
max_finalWorth = FOREACH grouped_data {
    max_finalWorth_in_group_category = FILTER max_finalWorth_in_group BY category == group;
    max_finalWorth_record = FILTER data_filtered BY finalWorth == max_finalWorth_in_group_category.max_finalWorth;
    max_finalWorth_personName = FOREACH max_finalWorth_record GENERATE FLATTEN(personName) AS max_finalWorth_personName;
    max_finalWorth_personName_limited = LIMIT max_finalWorth_personName 1;
    GENERATE group AS category, FLATTEN(max_finalWorth_personName_limited) AS max_finalWorth_personName;
}