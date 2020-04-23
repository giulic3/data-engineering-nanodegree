## Instructions

[] Step 1: Scope the Project and Gather Data
Since the scope of the project will be highly dependent on the data, these two things happen simultaneously. In this step, you’ll:

Identify and gather the data you'll be using for your project (at least two sources and more than 1 million rows). See Project Resources for ideas of what data you can use.
Explain what end use cases you'd like to prepare the data for (e.g., analytics table, app back-end, source-of-truth database, etc.)

[] Step 2: Explore and Assess the Data
Explore the data to identify data quality issues, like missing values, duplicate data, etc.
Document steps necessary to clean the data

[] Step 3: Define the Data Model
Map out the conceptual data model and explain why you chose that model
List the steps necessary to pipeline the data into the chosen data model

[] Step 4: Run ETL to Model the Data
Create the data pipelines and the data model
Include a data dictionary
Run data quality checks to ensure the pipeline ran as expected
Integrity constraints on the relational database (e.g., unique key, data type, etc.)
Unit tests for the scripts to ensure they are doing the right thing
Source/count checks to ensure completeness

[] Step 5: Complete Project Write Up
What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
Clearly state the rationale for the choice of tools and technologies for the project.
Document the steps of the process.
Propose how often the data should be updated and why.
Post your write-up and final data model in a GitHub repo.
Include a description of how you would approach the problem differently under the following scenarios:
If the data was increased by 100x.
If the pipelines were run on a daily basis by 7am.
If the database needed to be accessed by 100+ people.

## Additionals

[] Work with large amounts of data.
[] Combine datasets that are difficult to combine.
[] Enrich the data from several disparate sources.
[] Include recommendations for how to use to data to come up with insights.
[] Write a blog post about your project and link to it.

## Useful resources:
* Google: Dataset Search
* Kaggle Datasets
* Github: Awesome Public Datasets
* Data.gov
* Dataquest: 18 places to find data sets for data science projects
* KDnuggets: Datasets for Data Mining and Data Science
* UCI Machine Learning Repository
* Reddit: r/datasets/