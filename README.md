# StackOverflow-Spark
Analysis of stackoverflow users data with Spark

# StackOverflow with Apache Spark #

## Setup ##

- Run `sbt package` 
- Run `spark2-submit --class StackOverflow --packages com.databricks:spark-xml_2.11:0.4.1 --num-executors <x> <Your .jar file> <parameter>`
