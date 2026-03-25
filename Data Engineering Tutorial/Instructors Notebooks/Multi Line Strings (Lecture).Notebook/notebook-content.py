# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6f891a7e-aab7-4e91-a4ee-786056851776",
# META       "default_lakehouse_name": "TutorialLakehouse",
# META       "default_lakehouse_workspace_id": "c0a6c243-f3d9-4994-9b88-5172c1d903b8"
# META     }
# META   }
# META }

# CELL ********************

'''
This is a string
across multiple lines
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Specifying the schema using ddl

path_users = "Files/retail_dataset_de/users.csv"

schema_users = ''' 
                id INT, 
                created_at TIMESTAMP, 
                name STRING, 
                email STRING, 
                city STRING, 
                state STRING, 
                zip STRING, 
                birth_date DATE, 
                source STRING
                '''

# Spreading the schema across multiple lines
df_users = spark.read.option("header","true").\
                        schema(schema_users).\
                        format("csv").\
                        load(path_users)

display(df_users)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_users = "Files/retail_dataset_de/users.csv"

# multi line schema
df_users = spark.read.option("header","true").\
                        schema(''' id INT, 
                                    created_at TIMESTAMP, 
                                    name STRING, 
                                    email STRING, 
                                    city STRING, 
                                    state STRING, 
                                    zip STRING, 
                                    birth_date DATE, 
                                    source STRING
                                ''').\
                        format("csv").\
                        load(path_users)

display(df_users)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
