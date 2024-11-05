# Databricks notebook source
# MAGIC %md
# MAGIC 1. Aggregates with summation, subtraction 
# MAGIC 2. Check if its summation and run set of prompts function
# MAGIC 3. If its subtraction run the set of prompts function
# MAGIC 4. Run Initial validation function
# MAGIC 5. Store SQL to the Transpiler table.
# MAGIC 6. Go to the Parent table

# COMMAND ----------

# DBTITLE 1,Read pre-process data
import pyspark.sql.functions as F
from pyspark.sql.functions import col

#preprocess_table  = spark.sql("select * from agg_poc.fds.transpiler_equation_preprocess where aggregate_name IN ('RMUVPRRALOFPWLOA','RMUVPERBKOFPWLOA','RMUVPERBSOFPWLOA')")
preprocess_table  = spark.sql("select * from stratus_bronze_dev.ts1_stratus_targetdatamodel.transpiler_equation_preprocess where aggregate_name IN ('RMUM£ERALHH£WLAHUF')")


summation_equation = preprocess_table.filter(
    (F.size(F.col('operators')) <= 1) & ((F.size(F.col('operators')) == 0) | F.array_contains(F.col('operators'), '+')) & (F.col('aggfunction').isNotNull())
)
display(summation_equation)

simpleaggregate_equation = {}
rows = summation_equation.collect()

for row in rows:
    aggregate_name = row['aggregate_name']
    if aggregate_name not in simpleaggregate_equation:
        simpleaggregate_equation[aggregate_name] = []
    
    simpleaggregate_equation[aggregate_name] = {
        'original_equation': row['original_equation'],
        'operators': row['operators'],
        'box_codes': row['box_codes'],
        'bank_groups': row['bank_groups'],
        'execution_order': row['execution_order']
    }
display(simpleaggregate_equation)

# COMMAND ----------

# MAGIC %md
# MAGIC _Submission table and flash report views schemas were reterived and passed into the OPEN AI Prompts_

# COMMAND ----------

# DBTITLE 1,schema reterival
# # Load the table schema data into a Spark DataFrame
# schema_df = spark.sql("DESCRIBE fdscatalog.fds.dq_mapping") ### Try this with entire catalog 

# # Convert the DataFrame to a JSON string
# schema_json = schema_df.toPandas().to_json(orient='records')

# # Prepare the content for Azure OpenAI
# schema_content = "Table schema data: " + str(schema_json)

import json
from pyspark.sql import functions as F

# List only required tables
required_tables = ['flash_report_bankgroup_aggregates']
tables_df = spark.sql("SHOW TABLES IN stratus_bronze_dev.ts1_stratus_targetdatamodel").filter(F.col('tableName').isin(required_tables))

# Initialize a dictionary to store schemas
schemas = {}

# Iterate over tables and fetch schema
for row in tables_df.collect():
    table_name = row['tableName']
    schema_df = spark.sql(f"DESCRIBE TABLE stratus_bronze_dev.ts1_stratus_targetdatamodel.{table_name}")
    
    # Convert schema DataFrame to a list of dictionaries
    schema_list = schema_df.toPandas().to_dict(orient='records')
    
    # Store the schema in the dictionary
    schemas[table_name] = schema_list

# Convert the schemas dictionary to a JSON string
schemas_json = json.dumps(schemas, indent=4)

# Display the JSON
#print(schemas_json)




# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC _Open AI prompting is designed according to the complexity of the Aggregates. We have already collected the Metadata for each equation and based on the complexity Prompts will be changed_

# COMMAND ----------

# DBTITLE 1,Simple Prompts
import re
# Function to generate Box code SQL queries using GPT-4o
def box_to_mapping(input_data):

    # Convert the dictionary to a string format for the prompt
    input_data_str = str(input_data)
    
    # Formulate the prompt for GPT-4
    prompt1 = f"""
    You are an assistant that generates SQL queries based on dictionary input. The input dictionary contains keys that represent some identifier, and each key has a list of values. These values correspond to specific rows in a database.

    For each list in the dictionary, generate a single SQL query using the `IN` clause to retrieve all relevant rows from a table.

    Assume the following:
    - The table to query from is `stratus_bronze_dev.ts1_stratus_targetdatamodel.dps_mapping`
    - The column to match against is `BoxCode`.

    The input dictionary is: {input_data_str}

    Generate the SQL queries for each list:
    """
    
    response = client.chat.completions.create(
        messages=[
            {"role": "system", "content": "You are an AI model that helps with SQL and data processing."},
            {"role": "user", "content": prompt1}
        ],
        model="gpt-4o",
        max_tokens=1000
        #temperature=0.5
    )
    # Extract the text content from the response
    response_text = response.choices[0].message.content.strip()
    
    # Use regular expression to extract SQL statements from the response text
    sql_statements = re.findall(r"SELECT.*?;", response_text, re.DOTALL)
    
    # Update the input dictionary with the corresponding SQL statements
    input_json = json.dumps([{"aggregate_name": key, "Equation": value, "SQL_Query": sql_statements[i] if i < len(sql_statements) else None} for i, (key, value) in enumerate(input_data.items())], indent=4)

    return input_json

# Function to generate Final SQL queries for Aggregate equation using GPT-4o
def generate_sql_select_statements(sql_queries_dict,finalquery_dict):
    # Prepare the prompt
    prompt2 = f"""
    You are an SQL assistant. I have a list of SQL results stored in a dictionary. Here is the sample dictionary structure,
    """    [{'aggregate_name': 'RMUM£ERALHH£WLAHUF', 'Equation': {'original_equation': '[BX.ERA56A]ERLEVEL+[BX.ERA56B]ERLEVEL+[BX.ERA56C]ERLEVEL+[BX.ERA52F]ERLEVEL+[BX.ERA52G]ERLEVEL', 'operators': ['+'], 'box_codes': ['ERA56A', 'ERA56B', 'ERA56C', 'ERA52F', 'ERA52G'], 'bank_groups': [], 'execution_order': 'summation'}, 'SQL_Query': "SELECT *\nFROM stratus_bronze_dev.ts1_stratus_targetdatamodel.dps_mapping\nWHERE BoxCode IN ('ERA56A', 'ERA56B', 'ERA56C', 'ERA52F', 'ERA52G');", 'result': [{'BoxCode': 'ERA52F', 'TableCode': 'ER.02.01.01', 'RowCode': 'R0220', 'ColumnCode': 'C0010', 'MET': None, 'CUD': None, 'BAS': 'eba_BA:x6', 'CPS': 'boe_eba_CT:x9028', 'RCP': 'eba_GA:GB', 'RPR': 'boe_eba_RP:x9006', 'MCB': 'boe_eba_MC:x9345', 'CLS': 'eba_CG:x21', 'CPZ': None, 'IFP': 'boe_eba_TI:x6001', 'MBC': None, 'MCA': None, 'MCE': None, 'MCG': None, 'MCJ': None, 'MCQ': 'boe_eba_MC:x5074', 'MCY': 'eba_MC:x469', 'MPT': 'boe_eba_MC:x9373'}]"""

    For each result set, generate a SQL SELECT statement using 'flash_report_boxcode_aggregates' as the table name.
    The SELECT statement should include a WHERE clause that combines common column names with an OR condition if a column has multiple values.
    
    Here is the dictionary:

    {{{sql_queries_dict}}}

    For each 'result' entry in the dictionary, generate a SELECT statement that could be used to filter for these values in 'table1'.
    Please output the full SELECT statement for each dictionary entry separately.
    
    Here is the example to learn from :
    Input dict:
    'result': [
           {{'column1': 'value1', 'column2': 'value2'}},
            {{'column1': 'value3', 'column2': 'value4'}}
        ]
    Expected sql output for the result set:
    SELECT * FROM table1 WHERE (column1 = 'value1' OR column1 = 'value3') AND (column2 = 'value2' OR column2 = 'value4');"""

    
    # Make the API call to GPT-4
    response =  client.chat.completions.create(
      model="gpt-4o",
      max_tokens=4000,
      messages=[
            {"role": "system", "content": "You are an expert in SQL query construction."},
            {"role": "user", "content": prompt2}
        ]
    )
    
    # Extract the SQL SELECT statements from the response
    response_text = response.choices[0].message.content.strip()
    sql_statements = re.findall(r"SELECT \*.*?;", response_text, re.DOTALL)
    #sql_statements = re.findall(r"SELECT\s+[\s\S]*?\s+FROM\s+[\s\S]*?(?=;|$)", response_text, re.IGNORECASE | re.DOTALL)
    # Update the input dictionary with the corresponding SQL statements
    input_json = json.dumps([{"aggregate_name": key, "Equation": value, "SQL_Query": sql_statements[i] if i < len(sql_statements) else None} for i, (key, value) in enumerate(finalquery_dict.items())], indent=4)


    return input_json



# COMMAND ----------

def prompt2_input_clean(sql_queries_from_mappingtable):
    # Convert the sql_queries string to a dictionary
    sql_queries_dict = json.loads(sql_queries_from_mappingtable)
    columns_to_exclude = ['DPS']
    # Iterate over each dictionary in the list
    for entry in sql_queries_dict:
        sql = entry['SQL_Query']
        key = entry['aggregate_name']
        
        try:
            # Execute the SQL statement using Databricks' spark.sql()
            result_df = spark.sql(sql)
            result_df = result_df.drop(*columns_to_exclude)
            # Collect the result as a list of dictionaries (or any other format you prefer)
            result_data = result_df.collect()
            
            # Store the result back in the dictionary
            entry['result'] = [row.asDict() for row in result_data]
            entry['error'] = None
            
            print(f"Execution successful for key '{key}'")
        except Exception as e:
            # Handle any SQL execution errors
            entry['result'] = None
            entry['error'] = str(e)
            print(f"Error executing SQL for key '{key}': {e}")
            display(result_df)
    return (sql_queries_dict)

# COMMAND ----------

# DBTITLE 1,Main
import json

#Function call to reterive box codes from mapping table (Prompt1)
sql_queries = box_to_mapping(simpleaggregate_equation)

sql_queries_dict = prompt2_input_clean(sql_queries)
# Above function output is feed into the below function for Prompt2 to generate aggregate equation

sql_select_statements1 = generate_sql_select_statements(sql_queries_dict,simpleaggregate_equation)
print(sql_select_statements1)


# COMMAND ----------

import json

to_format = """[{'aggregate_name': 'RMUM£ERALHH£WLAHUF', 'Equation': {'original_equation': '[BX.ERA56A]ERLEVEL+[BX.ERA56B]ERLEVEL+[BX.ERA56C]ERLEVEL+[BX.ERA52F]ERLEVEL+[BX.ERA52G]ERLEVEL', 'operators': ['+'], 'box_codes': ['ERA56A', 'ERA56B', 'ERA56C', 'ERA52F', 'ERA52G'], 'bank_groups': [], 'execution_order': 'summation'}, 'SQL_Query': "SELECT *\\nFROM stratus_bronze_dev.ts1_stratus_targetdatamodel.dps_mapping\\nWHERE BoxCode IN ('ERA56A', 'ERA56B', 'ERA56C', 'ERA52F', 'ERA52G');", 'result': [{'BoxCode': 'ERA52F', 'TableCode': 'ER.02.01.01', 'RowCode': 'R0220', 'ColumnCode': 'C0010', 'MET': None, 'CUD': None, 'BAS': 'eba_BA:x6', 'CPS': 'boe_eba_CT:x9028', 'RCP': 'eba_GA:GB', 'RPR': 'boe_eba_RP:x9006', 'MCB': 'boe_eba_MC:x9345', 'CLS': 'eba_CG:x21', 'CPZ': None, 'IFP': 'boe_eba_TI:x6001', 'MBC': None}]}]"""

# Convert the string to a proper JSON structure
json_structure = json.loads(to_format.replace("'", '"'))

# Display the JSON structure
display(json_structure)

# COMMAND ----------

# DBTITLE 1,SQL to table for Aggregation Solution
from pyspark.sql.functions import expr, col
from pyspark.sql.functions import regexp_replace, concat, lit, to_date, when
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType

schema = StructType([
    StructField('aggregate_name', StringType(), True),
    StructField('Equation',  MapType(StringType(), StringType()), True),
    StructField('SQL_Query', StringType(), True),
    StructField('REPORTING_DATE', StringType(), True)
])

data = json.loads(sql_select_statements1)
df = spark.createDataFrame(data, schema)

# Define the column name you want to sum in the SQL query
column_to_sum = "POSITION"

# Use regexp_replace to replace 'SELECT *' with 'SELECT SUM(column_to_sum)' in SQL_Query
dfa = df.withColumn("SQL_Query", regexp_replace(col("SQL_Query"), "SELECT \\*", f"SELECT SUM({column_to_sum}) as aggregate_result"))

dfa = dfa.withColumn("SQL_Query", concat(regexp_replace(col("SQL_Query"), ";", ""), lit(" AND REPORTING_DATE = '2024-09-01'")))

# Check if 'bank_groups' column exists and is not null
if 'bank_groups' in dfa.columns:
    dfa = dfa.withColumn("SQL_Query", when(col("bank_groups").isNotNull(), 
                                           concat(regexp_replace(col("SQL_Query"), ";", ""), lit(" AND BANK IN ( 'BANK10','BANK11','BANK12')")))
                                           .otherwise(col("SQL_Query")))

dfa = dfa.withColumn("REPORTING_DATE", to_date(lit("2024-09-01")))

# Show updated DataFrame
display(dfa)

# COMMAND ----------

# DBTITLE 1,SQL store into the unity catalog table
# Here Generated SQL script will be stored in the Transpiler table
# Metadata details have to be included like version of the prompt, datetime, aggregate category - this will help us in the dedup steps.
# NEED MODEL DESIGN FOR THIS TABLE

# Rename one of the 'Equation' fields to a unique name
#final_df = dfa.withColumnRenamed('Equation', 'Equation_1')

dfa.write.mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("stratus_bronze_dev.ts1_stratus_targetdatamodel.transpiler_sql_script")

# COMMAND ----------

# DBTITLE 1,Run the Unit test Notebook - Handles seperately
# # Pass the equation file path as a parameter to the notebook
# def run_with_retry(notebook, timeout, args={}, max_retries=2):
#     num_retries = 0
#     while True:
#         try:
#             return dbutils.notebook.run(notebook, timeout, args)
#         except Exception as e:
#             if num_retries >= max_retries:
#                 raise e
#             else:
#                 print(f"Retrying error: {e}")
#                 num_retries += 1

# # Trigger another notebook with retry mechanism
# result = run_with_retry(
#     "/Workspace/Users/prabhavathi.muthusenapathy@bankofengland.co.uk/Work_in_progress/Transpiler_Unit_Testing",
#     60
# )

# COMMAND ----------

# import json
# result_data = {'status':'success'}
# dbutils.notebook.exit(json.dumps(result_data))
