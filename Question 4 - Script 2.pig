-- Register and load the PiggyBank
REGISTER '/home/hadoop/pig/piggybank.jar';
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load the CSV dataset
data = LOAD '/assignment/billionaires.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray);

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














DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load the CSV dataset
data = LOAD '/big_data_management/billionaire.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray);

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













DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load the CSV dataset
data = LOAD '/big_data_management/billionaire.csv' USING CSVLoader() AS (rank:int, finalWorth:int, category:chararray, personName:chararray);

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




max_finalWorth = FOREACH grouped_data {
    max_finalWorth_in_group = MAX(data_filtered.finalWorth);
    count_in_group = COUNT(data_filtered.personName)

    GENERATE group as category, FLATTEN(max_finalWorth_in_group) as max_finalWorth_in_group, FLATTEN(count_in_group) as count_personName;
}