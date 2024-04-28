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

-- Calculate the total worth
worth_total = FOREACH (GROUP data_filtered ALL) GENERATE SUM(data_filtered.finalWorth) AS total_worth;

-- Calculate metrics for each group
grouped_metrics = FOREACH grouped_data {
	category_worth_total = data_filtered.finalWorth;
	sorted_data = ORDER data_filtered BY finalWorth DESC;
	top_billionaire = LIMIT sorted_data 1;
	GENERATE group as category, 
		COUNT(data_filtered.finalWorth) AS billionaires_count,
		SUM(data_filtered.finalWorth) AS category_worth_total,
		--MAX(data_filtered.finalWorth) AS category_worth_max,
		AVG(data_filtered.finalWorth) AS category_worth_average,
		FLATTEN(top_billionaire.personName) AS top_billionaire;
}
	
-- Join the grouped metrics with the total worth
metrics_with_total = CROSS grouped_metrics, worth_total;

-- Produce the final data, by calculating the category worth percentage as well
final_data = FOREACH metrics_with_total GENERATE category, ((float)grouped_metrics::category_worth_total/(float)worth_total::total_worth) * 100 AS category_worth_pct, billionaires_count, category_worth_total, category_worth_average, top_billionaire;

-- Export the data
STORE final_data INTO '/output/category_metrics' USING PigStorage(',');
