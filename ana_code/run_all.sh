#!/bin/bash

USER_NAME=lb4140_nyu_edu

echo "Cleaning old HDFS outputs..."
hdfs dfs -rm -r /user/$USER_NAME/clean_with_show_output
hdfs dfs -rm -r /user/$USER_NAME/clean_recent_output
hdfs dfs -rm -r /user/$USER_NAME/season_output_full
hdfs dfs -rm -r /user/$USER_NAME/season_output_recent
hdfs dfs -rm -r /user/$USER_NAME/comparison_table_all
hdfs dfs -rm -r /user/$USER_NAME/comparison_table_no_big

echo "Running CleanWithShow..."
hadoop jar broadway_analytics.jar CleanWithShow \
/user/$USER_NAME/input/broadway.csv \
/user/$USER_NAME/clean_with_show_output

echo "Running CleanRecent..."
hadoop jar broadway_analytics.jar CleanRecent \
/user/$USER_NAME/input/Broadway2016_26.csv \
/user/$USER_NAME/clean_recent_output

echo "Running SeasonStats (full)..."
hadoop jar broadway_analytics.jar SeasonStats \
/user/$USER_NAME/clean_with_show_output \
/user/$USER_NAME/season_output_full

echo "Running SeasonStats (recent)..."
hadoop jar broadway_analytics.jar SeasonStats \
/user/$USER_NAME/clean_recent_output \
/user/$USER_NAME/season_output_recent

echo "Running Spark comparison tables..."
spark-submit make_comparison_tables.py

echo "Pipeline complete!"