# PySpark-Berlin

Understanding Apache Spark and PySpark

# Demystifying Apache Spark and PySpark: A Data Engineering Journey

Embarking on a journey through the world of Data Engineering, I discovered the power of Apache Spark and PySpark. To get really familiar with the concept, I wanted to learn the theory, simplify it in layman's terms, and then put it into practice via code. My goal? To break down the complexities of Apache Spark and PySpark into understandable concepts and demonstrate their practical application through a mini-project. This article aims to illuminate the quality of these tools and their profound impact on big data analysis.

## Understanding Apache Spark

### Technical Definition
Apache Spark, as defined by Databricks, is an open-source analytics engine for large-scale data processing. It handles both batch and real-time data workloads and provides native bindings for Java, Scala, Python, and R. Its core features include distributed task dispatching, scheduling, and I/O operations, primarily through the Resilient Distributed Dataset (RDD) model.

### Layman's Terms Definition
In simpler terms, Apache Spark is a tool for rapidly processing vast amounts of data. It achieves this by distributing data and tasks across numerous computers, allowing for quicker processing than would be possible on a single machine.

### An Analogy
Imagine Apache Spark as a team of workers handling a large set of tasks. Just as a manager would distribute work among various employees, Spark splits data processing tasks among multiple computers, significantly speeding up the process.

## PySpark: Python Meets Spark

### Technical Definition
PySpark is the Python API for Apache Spark, designed to bridge the gap between Spark's Scala foundation and Python's widespread popularity in data science.

### Layman's Terms Definition
PySpark is essentially Apache Spark's Python version. It enables Python users to leverage Spark's robust data handling and processing capabilities without needing to learn a new programming language.

### An Analogy
For Marvel fans, consider Apache Spark as Thor's hammer (Mjolnir), symbolizing its powerful data processing abilities. PySpark is similar to Captain America wielding Mjolnir, representing Python users' ability to harness Spark's power in a familiar environment.

## Practical Application: Analyzing Football Data with PySpark

### The Project
To illustrate the power of PySpark, I embarked on a project analyzing football players' goal conversion rates. The question: Who was the most efficient striker in the top European clubs during the 2018/2019 season? Using a dataset from Kaggle and Databricks, a unified analytics platform, the goal was to calculate the efficiency of players in converting shots into goals.

### Detailed Analysis of the PySpark Script
The PySpark script provided for analyzing football players' goal conversion rates can be dissected into several key steps. Here's a walkthrough of the process:

#### 1. Initialising the Spark Session and Importing Functions
- **Import Libraries:** The script starts by importing necessary PySpark libraries, particularly `pyspark.sql.functions` for various data manipulation tasks.
- **Creating Spark Session:** A SparkSession is initiated with the name "goal conversion metric," which acts as the entry point for PySpark functionalities.
- **Setting Log Level:** The log level is set to "WARN" to minimize verbose output during execution.

#### 2. Data Loading and Schema Inspection
- **Loading Data:** The script loads four datasets from Databricks FileStore: `appearances`, `season`, `leagues`, and `players`. These datasets contain information about player appearances, game details, league information, and player profiles.
- **Schema Inspection:** The script prints the schema of each dataset to understand the data structure, types of columns, and their significance.

#### 3. Data Cleaning and Transformation
- **Filtering Positions:** The script first filters the `appearances` dataset to include only specific player positions (Forwards: FW, FWR, FWL).
- **Dropping Unnecessary Columns:** It then removes columns from each dataset that are not relevant to the analysis.
- **Renaming Columns:** The script renames conflicting column names (e.g., changing 'name' to 'player_name' in the `players` dataset) to avoid ambiguity during data joins.

#### 4. Joining Datasets and Aggregation
- **Joining Tables:** The `appearances` dataset is joined with the `season` dataset based on the `gameID`. This is done to align player performance data with the respective seasons.
- **Grouping and Aggregating:** The data is then grouped by `playerID`, `leagueID`, and `season`, and aggregate functions are used to calculate the total goals and shots for each player.

#### 5. Additional Filtering and Data Preparation
- **Minimum Shot Threshold:** The script applies a filter to include only players who took a minimum number of shots (set to 20), ensuring that the analysis focuses on active strikers.
- **Filtering by Season:** The data is filtered to focus on a specific season (2019), allowing for a targeted analysis.

#### 6. Final Data Aggregation and Analysis
- **Joining Player and League Data:** The script joins the league and player data to the aggregated shooting data for a comprehensive view.
- **Calculating Goal Conversion Rate:** The goal conversion rate is calculated as the ratio of total goals to total shots for each player. This metric is then ordered in descending fashion to identify the most efficient players.

#### 7. Insights and Observations
The final output reveals the players with the highest goal conversion rates, offering insights into player efficiency. For instance, Kai Havertz emerged as a top performer in this analysis, a statistic that perhaps foreshadowed his move to Chelsea.

## Conclusion 
While the current analysis focuses on a relatively small dataset, the true strength of PySpark lies in its scalability. This same program could be extended to cover multiple seasons, allowing for a more comprehensive and large-scale data analysis. PySpark, powered by Apache Spark, is designed to handle vast datasets efficiently. It achieves this by distributing computational tasks across numerous computers in a cluster. This capability not only speeds up processing times but also enables the handling of significantly larger datasets without compromising performance.

## Insights and Learning Outcomes
This project serves as more than just a practical application of PySpark. It stands as a valuable learning experience in data processing and analysis using Apache Spark. The insights gleaned from the football data provide intriguing perspectives on player performance and efficiency. Moreover, the project highlights the extensive capabilities of PySpark in handling and analyzing big data. It reinforces the importance of hands-on experience in mastering new technologies and the power of diving into practical projects to unravel the potential of advanced data engineering tools.

## Sources
- [Databricks Glossary](https://www.databricks.com/glossary)
- [Kaggle Football Database](https://www.kaggle.com/datasets/technika148/football-database)
- [Understanding Apache Spark and PySpark](https://medium.com/@wiajayi/understanding-apache-spark-and-pyspark-4ee4ca377434)



