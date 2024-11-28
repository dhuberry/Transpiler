# Databricks notebook source
# DBTITLE 1,non flow - medium
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField
import re
import json

def parse_equation_medium(equation):
    split_pattern = re.compile(r'(\[B\.[^\]]+\]\[BX\.[^\]]+\]|\([^)]*\)|\d+\.\d+|[\*\-\+\(\)])')
    split_eqn = [item.strip() for item in split_pattern.split(equation) if item and item.strip()]
    
    # Create a dictionary to store parts and their execution order
    equation_dict = {"parts": [], "execution_order": []}

    # Process each part and store in the dictionary
    for idx, part in enumerate(split_eqn):
        part = part.strip()  # Remove leading/trailing spaces

        # Identify subqueries (e.g., [b.bank1][bx.box1])
        if re.match(r'\[B\.[^\]]+\]\[BX\.[^\]]+\]', part):
            bank_name = re.search(r'\[B\.([^\]]+)\]', part).group(1)
            box_name = re.search(r'\[BX\.([^\]]+)\]', part).group(1)
            equation_dict["parts"].append({
                "part": part,
                "type": "Subquery",
                "bank": bank_name,
                "sql": f"(SELECT * FROM stratus_bronze_dev.ts1_stratus_targetdatamodel.dps_mapping where BoxCode IN ('{box_name}'))"
            })

        # Identify expressions in parentheses (e.g., ([b.bank2][bx.box2] + [b.bank3][bx.box3]))
        elif part.startswith('(') and part.endswith(')'):
            inner_expression = part[1:-1]  # Remove parentheses
            # Extract bank and box names and build SQL query
            banks = re.findall(r'\[B\.([^\]]+)\]', inner_expression)
            boxes = re.findall(r'\[BX\.([^\]]+)\]', inner_expression)
            # Transform inner subqueries to sql
            inner_sql = ' + '.join([f"(SELECT * FROM stratus_bronze_dev.ts1_stratus_targetdatamodel.dps_mapping where BoxCode IN ('{box})')" for box in boxes])
            equation_dict["parts"].append({
                "part": part,
                "type": "ParenthesisExpression",
                "bank": tuple(banks),  # Store banks as a tuple
                "sql": f"({inner_sql})"
            })

        # Identify constants (e.g., 0.01)
        elif re.match(r'\d+\.\d+', part):
            equation_dict["parts"].append({
                "part": part,
                "type": "Constant",
                "sql": part
            })

        # Identify operators (*, -, +)
        else:
            equation_dict["parts"].append({
                "part": part,
                "type": "Operator",
                "sql": None
            })

        # Track execution order (sequence)
        equation_dict["execution_order"].append(idx)
    
    return json.dumps(equation_dict)

# Define a UDF to apply the parsing function
parse_equation_udf = udf(parse_equation_medium, StringType())

# Apply the UDF to the DataFrame
result_df = equation.withColumn('parsed', col('Equation'))

# Filter rows where the equation contains scalar values like 0.024
filtered_df = result_df.filter(F.col('Equation').rlike(r'\b\d+\.\d+\b'))

# Loop through each row in filtered_df and call the function
parsed_results = []
for row in filtered_df.collect():
    equation_str = row['EQUATION']
    parsed_result = parse_equation_medium(equation_str)
    parsed_results.append((row['AGGREGATE_NAME'], row['DESCRIPTION'], row['EQUATION'], row['Functions used'], row['FREQUENCY'], parsed_result))

# Create a new DataFrame with the parsed results
parsed_schema = StructType([
    StructField('Aggregate', StringType(), True),
    StructField('Description', StringType(), True),
    StructField('Equation', StringType(), True),
    StructField('Functions used', StringType(), True),
    StructField('FREQUENCY', StringType(), True),
    StructField('Parsed_dictionary', StringType(), True)
])

parsed_df = spark.createDataFrame(parsed_results, schema=parsed_schema)

# Display the DataFrame
display(parsed_df)

# COMMAND ----------

# DBTITLE 1,non - flow simple
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import re
import json


# Define a function to parse equations into terms and operators
# Extracting metadata from the equation and execution order is preserved
def parse_equation(equation):
    if equation is None:
        return [], [], 'summation', [], []
    all_brackets = re.findall(r'\[([^\]]+)\]', equation)
    bank_group = list(set([x.split('.', 1)[-1] for x in all_brackets if x.startswith("B.")]))
    box_codes = [x.split('.', 1)[-1] for x in all_brackets if x.startswith("BX.")]
    operators = list(set(re.findall(r'[\+\-\*/]', equation)))
    aggfunction = list(set(re.findall(r'(\w+)\(', equation)))
    if 'flow' in equation:
        aggfunction.append('flow')
    if 'BTFLOW' or 'BNFLOW' in equation:
        aggfunction.append('BTFLOW')
    execution_order = 'see execution order'
    return box_codes, operators, execution_order, bank_group, aggfunction

# Define a UDF (User Defined Function) to apply to each row
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, StructType, StructField

schema = StructType([
    StructField('original_equation', StringType(), True),
    StructField('operators', ArrayType(StringType()), True),
    StructField('box_codes', ArrayType(StringType()), True),
    StructField('bank_group', ArrayType(StringType()), True),
    StructField('execution_order', StringType(), True),
    StructField('aggfunction', ArrayType(StringType()), True)
])

@udf(schema)
def process_equation(equation):
    box_codes, operators, execution_order, bank_group, aggfunction = parse_equation(equation)
    if len(operators) > 1:
        execution_order = 'see execution order'
    elif '+' in operators:
        execution_order = 'summation'
    elif '*' in operators:
        execution_order = 'multiplication'
    elif '/' in operators:
        execution_order = 'division'
    elif '-' in operators:
        execution_order = 'subtraction'
    elif len(operators) == 0:
        execution_order = 'single box code Aggregate'
    return (equation, operators, box_codes, bank_group, execution_order, aggfunction)

# Process the DataFrame
result_df = equation.withColumn('parsed', process_equation(col('equation')))

# Explode the parsed column into separate columns
result_df = result_df.select(
    col('aggregate_name'),
    col('parsed.original_equation').alias('original_equation'),
    col('parsed.operators').alias('operators'),
    col('parsed.box_codes').alias('box_codes'),
    col('parsed.bank_group').alias('bank_group'),
    col('parsed.execution_order').alias('execution_order'),
    col('parsed.aggfunction').alias('aggfunction'),
    col('description'),
    col('Frequency')
)

# Collect the result into a Python dictionary
preprocess_equation = {}
rows = result_df.collect()

for row in rows:
    aggregate_name = row['aggregate_name']
    if aggregate_name not in preprocess_equation:
        preprocess_equation[aggregate_name] = []
    
    preprocess_equation[aggregate_name] = {
        'original_equation': row['original_equation'],
        'operators': row['operators'],
        'box_codes': row['box_codes'],
        'bank_group': row['bank_group'],
        'execution_order': row['execution_order'],
        'aggfunction': row['aggfunction']
    }
    description = row['description']
    frequency = row['Frequency']
display(result_df)

# COMMAND ----------

# DBTITLE 1,Flow
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import re
import json

# Sample data
file_path1 = 'dbfs:/FileStore/Aggregates/Medium_Aggregate_1_FLOW.csv'
df = spark.read.csv(file_path1, header=True, inferSchema=True).limit(100)

# Define a function to parse equations into terms and operators
# Extracting metadata from the equation and execution order is preserved
def parse_equation(equation):
    if equation is None:
        return [], [], 'summation', [], []
    all_brackets = re.findall(r'\[([^\]]+)\]', equation)
    bank_group = list(set([x.split('.', 1)[-1] for x in all_brackets if x.startswith("B.")]))
    box_codes = [x.split('.', 1)[-1] for x in all_brackets if x.startswith("BX.")]
    operators = list(set(re.findall(r'[\+\-\*/]', equation)))
    aggfunction = list(set(re.findall(r'(\w+)\(', equation)))
    if 'flow' in equation:
        aggfunction.append('flow')
    if 'BTFLOW' or 'BNFLOW' in equation:
        aggfunction.append('BTFLOW')
    execution_order = 'see execution order'
    return box_codes, operators, execution_order, bank_group, aggfunction

# Define a UDF (User Defined Function) to apply to each row
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, StructType, StructField

schema = StructType([
    StructField('original_equation', StringType(), True),
    StructField('operators', ArrayType(StringType()), True),
    StructField('box_codes', ArrayType(StringType()), True),
    StructField('bank_group', ArrayType(StringType()), True),
    StructField('execution_order', StringType(), True),
    StructField('aggfunction', ArrayType(StringType()), True)
])

@udf(schema)
def process_equation(equation):
    box_codes, operators, execution_order, bank_group, aggfunction = parse_equation(equation)
    if len(operators) > 1:
        execution_order = 'see execution order'
    elif '+' in operators:
        execution_order = 'summation'
    elif '*' in operators:
        execution_order = 'multiplication'
    elif '/' in operators:
        execution_order = 'division'
    elif '-' in operators:
        execution_order = 'subtraction'
    elif len(operators) == 0:
        execution_order = 'single box code Aggregate'
    return (equation, operators, box_codes, bank_group, execution_order, aggfunction)

# Process the DataFrame
result_df = df.withColumn('parsed', process_equation(col('equation')))

# Explode the parsed column into separate columns
result_df = result_df.select(
    col('aggregate_name'),
    col('parsed.original_equation').alias('original_equation'),
    col('parsed.operators').alias('operators'),
    col('parsed.box_codes').alias('box_codes'),
    col('parsed.bank_group').alias('bank_group'),
    col('parsed.execution_order').alias('execution_order'),
    col('parsed.aggfunction').alias('aggfunction'),
    col('description'),
    col('Frequency')
)

# Collect the result into a Python dictionary
preprocess_equation = {}
rows = result_df.collect()

for row in rows:
    aggregate_name = row['aggregate_name']
    if aggregate_name not in preprocess_equation:
        preprocess_equation[aggregate_name] = []
    
    preprocess_equation[aggregate_name] = {
        'original_equation': row['original_equation'],
        'operators': row['operators'],
        'box_codes': row['box_codes'],
        'bank_group': row['bank_group'],
        'execution_order': row['execution_order'],
        'aggfunction': row['aggfunction']
    }
    description = row['description']
    frequency = row['Frequency']
display(result_df)
