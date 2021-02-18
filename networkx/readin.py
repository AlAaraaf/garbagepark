import pyspark as ps
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as f
import pandas as pd

sc = SparkContext("local", "garbage")
spark = SparkSession(sc)
edge = pd.read_csv('edge.csv')
