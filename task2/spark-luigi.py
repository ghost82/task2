#!/usr/bin/python3.4
# -*- coding: utf-8 -*-
#
import luigi
from luigi.contrib.hdfs.target import HdfsTarget
from luigi.contrib.spark import PySparkTask, SparkSubmitTask

class SparkHr(SparkSubmitTask):
    """
    It runs a :py:class:`luigi.contrib.spark.SparkSubmitTask` task

    Example luigi configuration::

        [spark]
        spark-submit: /usr/local/spark/bin/spark-submit
        master: spark://localhost:7077
        deploy-mode: client
    """
    driver_memory = '100m'
    executor_memory = '100m'
    total_executor_cores = 4 

    name = "Spark HR Mean counter"
    app = "task2_2.10-1.0.jar"
    entry_class = "org.ghost.Task2"

    def app_options(self):
        # These are passed to the Spark main args in the defined order.
        return [self.input().path, self.output().path]

    def requires(self):
        return HdfsTarget("/user/root/data/")

    def output(self):
        return HdfsTarget("/user/root/data/")

if __name__ == '__main__':
    luigi.run(main_task_cls=SparkHr)
#    luigi.build([SparkHr() ])

