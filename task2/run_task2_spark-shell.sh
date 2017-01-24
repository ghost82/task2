hadoop fs -rm -R -f /user/root/data/hr_median.csv
spark-shell --master yarn-client --driver-memory 2g --executor-memory 2g --executor-cores 4 --packages com.databricks:spark-csv_2.11:1.2.0  -i task2.scala
hadoop fs -ls "/user/root/data/hr_median.csv"
hadoop fs -text /user/root/data/hr_median.csv/part-00000

