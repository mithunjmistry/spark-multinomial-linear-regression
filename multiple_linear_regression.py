# mmistry1@uncc.edu
# Mithun Mistry
# 800961418
import sys
import numpy as np
from pyspark import SparkContext

# Continue only if input file exists
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: linreg <datafile>"
        exit(-1)

    spark_context = SparkContext(appName="MultipleLinearRegression")

    #First element is y_i in each line. Other elements are x_i
    #This will make a pointer to loaded file so it can be made RDD later when an action is called.
    #RDD will be list of strings which will be each line of this file
    yx_input_file = spark_context.textFile(sys.argv[1])

    #Read every line with mapper
    #Split on ',' and get dependent and independent variables as list.
    #Example - [yi, xi1, x12, xi3, .... , xin]
    yx_lines = yx_input_file.map(lambda line: line.split(','))

    def first_key(values):
        # This function will calculate and give us product of X and X transpose matrices
        values[0] = 1.0  #For Beta = 0, make the first element zero as defined in X
        X = np.asmatrix(np.array(values).astype('float')).T #Make input as a float array -> matrix and take transpose with numpy libraries
        return np.dot(X, X.T)  #Product of X and X transpose

    def second_key(values):
        # This function will calculate and give us product of X and Y
        Y = float(values[0])  #Remove 1st element in Y
        values[0] = 1.0  #For Beta = 0, make the first element zero as defined in X
        # convert input into a float array, then to matrix
        X = np.asmatrix(np.array(values).astype('float')).T #Make input as a float array -> matrix with numpy libraries
        return np.multiply(X, Y)  #Product of X and Y (scalar)

    # We will pass product calculating function to mapper for every set
    # Here, set = [yi, xi1, xi2, xi3, ... , xin]
    # Reducer to sum product of X and X transpose
    first = np.asmatrix(yx_lines.map(lambda l: ("first_key", first_key(l))).reduceByKey(
        lambda x, y: np.add(x, y)).map(lambda l: l[1]).collect()[0])

    # We will pass another product calculating function to mapper for every set
    # Here, set = [yi, xi1, xi2, xi3, ... , xin]
    # Reducer to sum product of X and Y
    second = np.asmatrix(yx_lines.map(lambda l: ("second_key", second_key(l))).reduceByKey(
        lambda x, y: np.add(x, y)).map(lambda l: l[1]).collect()[0])

    # This will take the dot product of inverse of 'first' matrix and 'second' vector
    # and give us vector of co-efficients 
    # Also convert to list so we can iterate and print it
    beta = np.array(np.dot(np.linalg.inv(first), second)).tolist()

    #Print Multiple Linear Regression Co-efficients in the desired formatted output
    print 'Beta - '
    for coefficient in beta:
        print coefficient[0]

    spark_context.stop()
