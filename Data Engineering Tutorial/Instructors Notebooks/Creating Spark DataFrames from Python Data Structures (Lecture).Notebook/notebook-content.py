# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Creating Spark DataFrames from Python Data Structures
# ### Links and Resources
# - https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.createDataFrame.html
# - https://docs.python.org/3/tutorial/datastructures.html

# MARKDOWN ********************

# ### Lists
# Lists are ordered, mutable collections of items that can hold a mix of data types.  
# 
# For example:
# 
# This is a list stored in a variable called fruits
# 
# ```
# fruits = ["apple", "banana", "cherry"]
# ```
# 
# You can also have lists embedded within lists, in the below example, matrix, stores a list of list elements.
# 
# These can be referred to as multi-dimensional arrays or nested lists.
# 
# ```
# nested_list = [[1, 2, 3],[4, 5, 6],[7, 8, 9]]
# ```
#  
# 


# CELL ********************

nested_list = [[1, 2, 3],[4, 5, 6],[7, 8, 9]]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = ['col1', 'col2', 'col3']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.createDataFrame(nested_list, schema)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Tuples / Sets
# A tuple or set consists of a number of values separated by commas and enclosed in parenthesis, for instance:
# 
# ```
# fruits = ("apple", "banana", "cherry")
# ```
# 
# You can also have tuples embedded within lists.
# 
# ```
# nested_list_tuples = [(1, 2, 3),(4, 5, 6),(7, 8, 9)]

# CELL ********************

nested_list_tuples = [(1, 2, 3),(4, 5, 6),(7, 8, 9)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.createDataFrame(nested_list_tuples, schema).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dictionaries
# Python dictionaries (or JSON structures) store data in key-value pairs.
# 
# For example:
# 
# ```
# person = [{"name": "Alice", "age": 30}]
# ```
# 
# Here is a list of dictionary elements.
# 
# ```
# people = [
#     {"name": "Alice", "age": 30, "city": "New York"},
#     {"name": "Bob", "age": 25, "city": "Los Angeles"},
#     {"name": "Charlie", "age": 35, "city": "Chicago"}
# ]
# ```


# CELL ********************

people = [
    {"name": "Alice", "age": 30, "city": "New York"},
    {"name": "Bob", "age": 25, "city": "Los Angeles"},
    {"name": "Charlie", "age": 35, "city": "Chicago"}
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.createDataFrame(people).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
