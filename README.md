# spark-multinomial-linear-regression
Multinomial Linear Regression in distributed environment using Apache Spark (PySpark).

Files - 
1. multiple_linear_regression.py
2. /output/yxlin.out
3. /output/yxlin2.out
4. /input/yxlin.csv
5. /input/yxlin2.csv
--------------------------------------------------------------------------------------------------------
Assumption - 
1. The system already contains Spark, NumPy, PySpark, Python.
2. yxlin.csv and yxlin2.csv are already in HDFS.
--------------------------------------------------------------------------------------------------------
Execution - 
1. Issue the following command to run the linear regression - 
spark-submit multiple_linear_regression.py yxlin.csv
spark-submit multiple_linear_regression.py yxlin2.csv
---------------------------------------------------------------------------------------------------------
Output - 
As per the current program, output will printed in the console. Beta values will be printed.
Files are available in output folder as yxlin.out and yxlin2.out.
----------------------------------------------------------------------------------------------------------

View my portfolio on - 
www.mithunjmistry.com
