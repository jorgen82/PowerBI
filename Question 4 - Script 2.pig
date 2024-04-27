-- Register and load the PiggyBank
REGISTER 'hdfs://localhost:9000/pig/piggybank.jar';
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();


-- Load the CSV dataset
data = LOAD 'hdfs://localhost:9000/assignment/billionaires.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray);

-- Filter out rows where finalWorth is null
data_filtered = FILTER data BY finalWorth IS NOT NULL;

-- Group the filtered data by 'category'
grouped_data = GROUP data_filtered BY category;

-- Calculate the maximum 'finalWorth' for each category
max_finalWorth_in_group = FOREACH grouped_data GENERATE group AS category, MAX(data_filtered.finalWorth) AS max_finalWorth;

-- Find the 'personName' with the maximum 'finalWorth' in each category
max_finalWorth = FOREACH grouped_data {
    max_finalWorth_in_group_category = FILTER max_finalWorth_in_group BY category == group;
    max_finalWorth_record = FILTER data_filtered BY finalWorth == max_finalWorth_in_group_category.max_finalWorth;
    max_finalWorth_personName = FOREACH max_finalWorth_record GENERATE FLATTEN(personName) AS max_finalWorth_personName;
    max_finalWorth_personName_limited = LIMIT max_finalWorth_personName 1;
    GENERATE group AS category, FLATTEN(max_finalWorth_personName_limited) AS max_finalWorth_personName;
}

-- Count the number of 'personName' in each category
category_counts = FOREACH grouped_data GENERATE
    group AS category,
    COUNT(data_filtered.personName) AS person_count;

-- Calculate the total count of 'personName'
total_count = FOREACH (GROUP data_filtered ALL) GENERATE
    COUNT(data_filtered.personName) AS total_person_count;

-- Calculate the percentage of each category count over the total count
category_percentage = FOREACH category_counts GENERATE
    category,
    person_count AS category_count,
    (double)person_count / (double)(total_count.total_person_count) * 100 AS percentage;

-- Calculate the average 'finalWorth' of 'personName' in each category
category_average = FOREACH grouped_data GENERATE
    group AS category,
    AVG(data_filtered.finalWorth) AS avg_finalWorth;

-- Join all the results together
final_result = JOIN category_percentage BY category, category_average BY category, max_finalWorth BY category;

-- Store the results
STORE final_result INTO 'output_path';














-- Register and load the PiggyBank
REGISTER 'hdfs://localhost:9000/pig/piggybank.jar';
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();


-- Load the CSV dataset
data = LOAD 'hdfs://localhost:9000/assignment/billionaires.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray);

-- Filter out rows where finalWorth is null
data_filtered = FILTER data BY finalWorth IS NOT NULL;

-- Group the filtered data by 'category'
grouped_data = GROUP data_filtered BY category;

-- Calculate the maximum 'finalWorth' for each category
max_finalWorth_in_group = FOREACH grouped_data GENERATE
    group AS category,
    MAX(data_filtered.finalWorth) AS max_finalWorth;

-- Find the 'personName' with the maximum 'finalWorth' in each category
max_finalWorth = FOREACH grouped_data {
    max_finalWorth_record = FILTER data_filtered BY finalWorth == max_finalWorth_in_group.category;
    max_finalWorth_personName = FOREACH max_finalWorth_record GENERATE FLATTEN(personName) AS max_finalWorth_personName;
    max_finalWorth_personName_limited = LIMIT max_finalWorth_personName 1;
    GENERATE group AS category, FLATTEN(max_finalWorth_personName_limited) AS max_finalWorth_personName;
}

-- Count the number of 'personName' in each category
category_counts = FOREACH grouped_data GENERATE
    group AS category,
    COUNT(data_filtered.personName) AS person_count;

-- Calculate the total count of 'personName'
total_count = FOREACH (GROUP data_filtered ALL) GENERATE
    COUNT(data_filtered.personName) AS total_person_count;

-- Calculate the percentage of each category count over the total count
category_percentage = FOREACH category_counts GENERATE
    category,
    person_count AS category_count,
    (double)person_count / (double)(total_count.total_person_count) * 100 AS percentage;

-- Calculate the average 'finalWorth' of 'personName' in each category
category_average = FOREACH grouped_data GENERATE
    group AS category,
    AVG(data_filtered.finalWorth) AS avg_finalWorth;

-- Join all the results together
final_result = JOIN category_percentage BY category, category_average BY category, max_finalWorth BY category;

-- Store the results
STORE final_result INTO 'output_path';













-- Register and load the PiggyBank
REGISTER 'hdfs://localhost:9000/pig/piggybank.jar';
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();


-- Load the CSV dataset
data = LOAD 'hdfs://localhost:9000/assignment/billionaires.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray);

-- Filter out rows where finalWorth is null
data_filtered = FILTER data BY finalWorth IS NOT NULL;

-- Group the filtered data by 'category'
grouped_data = GROUP data_filtered BY category;

-- Calculate the maximum 'finalWorth' for each category
max_finalWorth = FOREACH grouped_data {
    -- Calculate the maximum 'finalWorth' for the current group
    max_finalWorth_in_group = MAX(data_filtered.finalWorth);

    -- Join the filtered data with the maximum 'finalWorth' for the current group
    max_finalWorth_data = JOIN data_filtered BY (category, finalWorth), grouped_data BY (group, max_finalWorth_in_group);

    -- Project the category and personName
    max_finalWorth_personName = FOREACH max_finalWorth_data GENERATE FLATTEN(data_filtered.category) AS category, FLATTEN(data_filtered.personName) AS personName;

    -- Limit to one record if multiple records have the same maximum 'finalWorth'
    max_finalWorth_personName_limited = LIMIT max_finalWorth_personName 1;

    -- Generate the result
    GENERATE FLATTEN(max_finalWorth_personName_limited);
}







-- Register and load the PiggyBank
REGISTER 'hdfs://localhost:9000/pig/piggybank.jar';
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();


-- Load the CSV dataset
data = LOAD 'hdfs://localhost:9000/assignment/billionaires.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray);

-- Filter out rows where category is null
data_filtered = FILTER data BY category IS NOT NULL AND NOT (category MATCHES '');;

-- Filter out rows where finalWorth is null
data_filtered = FILTER data BY finalWorth IS NOT NULL;

-- Group the filtered data by 'category'
grouped_data = GROUP data_filtered BY category;


-- Count the number of 'personName' in each category
billionaires_count = FOREACH grouped_data GENERATE
    group AS category,
    COUNT(data_filtered.personName) AS person_count;

-- Calculate the total count of 'personName'
billionaires_total = FOREACH (GROUP data_filtered ALL) GENERATE
    COUNT(data_filtered.personName) AS total_person_count;
    
-- Calculate the total worth
worth_total = FOREACH (GROUP data_filtered ALL) GENERATE
    SUM(data_filtered.finalWorth) AS total_worth;

-- Calculate the percentage of each category count over the total count
category_percentage = FOREACH billionaires_count GENERATE
    category,
    person_count AS category_count,
    (double)person_count / (double)(billionaires_total.total_person_count) * 100 AS percentage;

-- Calculate the total 'finalWorth' of 'personName' in each category
category_worth_total = FOREACH grouped_data GENERATE
    group AS category,
    SUM(data_filtered.finalWorth) AS sum_finalWorth;

-- Calculate the max 'finalWorth' of 'personName' in each category
category_worth_max = FOREACH grouped_data GENERATE
    group AS category,
    MAX(data_filtered.finalWorth) AS max_finalWorth;
       
-- Calculate the average 'finalWorth' of 'personName' in each category
category_worth_average = FOREACH grouped_data GENERATE
    group AS category,
    AVG(data_filtered.finalWorth) AS avg_finalWorth;

-- Calculate the pct of worth of each categorey over the total
pct = FOREACH category_worth_total GENERATE category, (double)sum_finalWorth/(double)worth_total.total_worth;
	
test_join = JOIN category_worth_total BY category FULL OUTER, worth_total;
	
-- Join all the results together
final_result = JOIN category_percentage BY category, category_worth_average BY category, max_finalWorth BY category;


----not working
test = FOREACH grouped_data {
	--billionaires_count = COUNT(data_filtered.personName);
	--worth_bag = data_filtered.finalWorth;
	--category_worth_total = SUM(worth_bag.finalWorth);
	category_worth_total = data_filtered.finalWorth;
	--category_worth_max = MAX(worth_bag.finalWorth);
	--category_worth_average = AVG(worth_bag.finalWorth);
	GENERATE group as category, 
		--billionaires_count AS billionaires_count,
		SUM(data_filtered.finalWorth) AS category_worth_total,
		category_worth_max AS category_worth_max,
		category_worth_average AS category_worth_average;
}
	
	
	
	
	FLATTEN(billionaires_count) as billionaires_count, FLATTEN(category_worth_total) as category_worth_total, FLATTEN(category_worth_max) as category_worth_max, FLATTEN(category_worth_average) as category_worth_average;
}




max_finalWorth = FOREACH grouped_data {
    max_finalWorth_in_group = MAX(data_filtered.finalWorth);
    count_in_group = COUNT(data_filtered.personName)

    GENERATE group as category, FLATTEN(max_finalWorth_in_group) as max_finalWorth_in_group, FLATTEN(count_in_group) as count_personName;
}
