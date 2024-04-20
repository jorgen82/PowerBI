DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load the CSV dataset
data = LOAD '/big_data_management/billionaire.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray);

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
total_count = FOREACH (GROUP category_counts ALL) GENERATE SUM(category_counts.count_people) as total_count;
 
-- Calculate the percentage of each category count over the total count
category_pct = FOREACH category_counts GENERATE category, count_people, ((double)count_people / (double)total_count.total_count) * 100 AS pct;

-- Calculate the average worth of people in each category
category_worth_avg = FOREACH grouped_data GENERATE group AS category, AVG(data.finalWorth) AS avg_worth;

--data_grouped_all = GROUP data ALL;
--max_person_worth = FOREACH data_grouped_all GENERATE MAX(data.finalWorth);

-- Find the person with the maximum worth in each category
category_max_worth = FOREACH grouped_data {
    max_person_worth = MAX(data.finalWorth);
    max_person_details = FILTER data BY finalWorth == max_person_worth;
    max_person_details = FOREACH max_person_details GENERATE personName, finalWorth;
    GENERATE
        group AS category,
        FLATTEN(max_person_details) AS (personName:chararray, finalWorth:double);
}


max_group_worth = FOREACH grouped_data GENERATE group as category, MAX(data.finalWorth) as max_finalWorth;

category_max_worth = FOREACH grouped_data {
    max_group_category_worth = FILTER max_group_worth by category == group;
    max_person_worth = FILTER data BY finalWorth == max_group_category_worth.max_finalWorth;
    max_person_details = FOREACH max_person_worth GENERATE FLATTEN(personName) AS max_person_details;
    max_person_details_limited = LIMIT max_person_details 1;
    GENERATE
        group AS category,
        FLATTEN(max_person_details_limited) AS max_finalWorth_personName;
}


max_finalWorth = FOREACH grouped_data {
	sorted_data = ORDER data BY finalWorth DESC;
	top_record = LIMIT sorted_data 1;
	GENERATE group AS category, FLATTEN(top_record.personName) as max_finalWorth_personName;
	}

-- Merge the results into one relation
merged_data = JOIN category_counts BY category, category_pct BY category, category_worth_avg BY category, max_finalWorth BY category;

-- Store the results
STORE merged_data INTO '/output/country_worth_cpi' USING PigStorage(',');

