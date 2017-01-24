hadoop fs -rm -R -f /user/root/data/hr_median.csv
spark-submit --master yarn-client --driver-memory 2g --executor-memory 2g --executor-cores 4 --packages com.databricks:spark-csv_2.11:1.2.0  --class org.ghost.Task2 task2/target/scala-2.10/task2_2.10-1.0.jar /user/root/data/ /user/root/data/
hadoop fs -ls "/user/root/data/hr_median.csv"
hadoop fs -text /user/root/data/hr_median.csv/part-00000

