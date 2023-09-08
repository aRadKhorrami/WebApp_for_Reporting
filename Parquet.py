import os.path#, time
import glob
#from dateutil import parser
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import configparser
import pyarrow.csv as csv

# Get the current working directory
current_directory = os.getcwd()

# Define the path to the config file
config_file_path = os.path.join(current_directory, 'config.ini')

# Check if the config file exists
if not os.path.exists(config_file_path):
    print("Config file not found in the current directory.")
else:
    # Read the configuration from the file
    config = configparser.ConfigParser()
    config.read(config_file_path)

# Retrieve the values from the configuration
root = config['Paths']['root']
subRoot_ = config['Paths']['subRoot']

# Split subRoot into a list
subRoot = subRoot_.split(', ')

# Create a Spark session
spark = SparkSession.builder.appName("ParquetQuery").getOrCreate()

for sR in subRoot:
    dirPath = root + sR

    # Use glob to list all CSV files in the directory
    csv_files = glob.glob(os.path.join(dirPath, '*.csv'))

    # Check if there are any CSV files in the directory
    if not csv_files:
        print("No CSV files found in the directory.")
    else:
        # Sort the files by modification time in descending order
        csv_files.sort(key=os.path.getmtime, reverse=True)

        # Get the most recently modified CSV file
        most_recent_file = csv_files[0]

        # Print the path of the most recently modified CSV file
        print("Most recently modified CSV file:", most_recent_file)

    data = pd.read_csv(most_recent_file,encoding = 'utf-8')

    # Convert pandas DataFrame to PyArrow Table
    table = pa.Table.from_pandas(data)

    # Path to the output Parquet file
    file_path, extension = os.path.splitext(most_recent_file)
    parquet_file_path = file_path + '.parquet2'

    # Write the PyArrow Table to Parquet format
    pq.write_table(table, parquet_file_path)

    # Load the Parquet data into DataFrames
    if sR == "EDWEB_DAILY_TOTAL_APN_REPORT/":
        apn_df = spark.read.parquet(parquet_file_path)
    elif sR == "EDWEB_DAY_MVPN_PREPAID/":
        mvpn_df = spark.read.parquet(parquet_file_path)
    elif sR == "GSM_SERVICE_MAST_CORP/":
        gsm_df = spark.read.parquet(parquet_file_path)
    else:
        print ("UnKnown directory/file")


# Define and create a temporary view for each DataFrame
gsm_df.createOrReplaceTempView("GSM")
apn_df.createOrReplaceTempView("APN")
mvpn_df.createOrReplaceTempView("MVPN")

# Define a Spark SQL query
query1 = """
SELECT DISTINCT msisdn_nsk
FROM (
    SELECT msisdn_nsk FROM GSM
    UNION
    SELECT CONCAT('98', mobl_num_voice_v) as msisdn_nsk FROM APN
    UNION
    SELECT CONCAT('98', msisdn) as msisdn_nsk FROM MVPN
) A
"""

# Execute the Spark SQL query
msisdn_list = spark.sql(query1)

# Define and create a temporary view for the DataFrame
msisdn_list.createOrReplaceTempView("msisdn_list")

query2 = """
SELECT distinct e.*, m.short_code, m.mvpn_status
FROM (
    SELECT c.*, d.actual_apn_v, d.status_v AS apn_status, d.ip_address_v
    FROM (
        SELECT a.msisdn_nsk, b.status_code_v AS msisdn_status, b.contract_type_v,
               b.company_name_v, b.package_code_v, b.economic_code_n, b.profile_id,
               b.kit_number_v, b.profile_manage
        FROM msisdn_list a
        LEFT JOIN GSM b ON a.msisdn_nsk = b.msisdn_nsk
    ) c
    LEFT JOIN APN d ON RIGHT(c.msisdn_nsk, 10) = d.mobl_num_voice_v
) e
LEFT JOIN MVPN m ON RIGHT(e.msisdn_nsk, 10) = m.msisdn
"""

total_EDW_reports = spark.sql(query2)

# Define and create a temporary view for the DataFrame
total_EDW_reports.createOrReplaceTempView("total_EDW_reports")

# Show the result
#total_EDW_reports.show()

# Write the DataFrame to a CSV file
#total_EDW_reports.write.csv("/home/ali/myGitRepos/WebApp_for_Reporting/keyboard700tomani/total_EDW_reports.csv", header=True, mode="overwrite")

# Define a Spark SQL query
query3 = """
select msisdn_nsk,msisdn_status,
COALESCE (
(select Max(b.contract_type_v) from APN b where b.mobl_num_voice_v = RIGHT(total_EDW_reports.msisdn_nsk,10)), Null
) as contract_type_v
, 
COALESCE (
(select Max(b.company_name_v) from APN b where b.mobl_num_voice_v = RIGHT(total_EDW_reports.msisdn_nsk,10)), Null
) as company_name_v
, 
COALESCE (
(select Max(b.package_code_v) from APN b where b.mobl_num_voice_v = RIGHT(total_EDW_reports.msisdn_nsk,10)), Null
) as package_code_v
, 
COALESCE (
(select Max(b.economic_code_n) from APN b where b.mobl_num_voice_v = RIGHT(total_EDW_reports.msisdn_nsk,10)), Null
) as economic_code_n
, profile_id
,  
COALESCE (
(select Max(b.kit_number_v) from APN b where b.mobl_num_voice_v = RIGHT(total_EDW_reports.msisdn_nsk,10)), Null
) as kit_number_v
,  
COALESCE (
(select Max(b.profile_manager) from APN b where b.mobl_num_voice_v = RIGHT(total_EDW_reports.msisdn_nsk,10)), Null
) as profile_manage, actual_apn_v,apn_status,ip_address_v,short_code,mvpn_status
from total_EDW_reports
where total_EDW_reports.contract_type_v is null

UNION

select * 
from total_EDW_reports
where total_EDW_reports.contract_type_v is not null
"""

total_EDW_reports = spark.sql(query3)
# Define and create a temporary view for each DataFrame
total_EDW_reports.createOrReplaceTempView("total_EDW_reports")

# Define a Spark SQL query
query4 = """
select msisdn_nsk,msisdn_status,
COALESCE (
(select Max(b.contract_type_v) from MVPN b where b.msisdn = RIGHT(total_EDW_reports.msisdn_nsk,10)), Null
) as contract_type_v
, 
COALESCE (
(select Max(b.company_name) from MVPN b where b.msisdn = RIGHT(total_EDW_reports.msisdn_nsk,10)), Null
) as company_name_v
, 
COALESCE (
(select Max(b.package_code_v) from MVPN b where b.msisdn = RIGHT(total_EDW_reports.msisdn_nsk,10)), Null
) as package_code_v

, 
COALESCE (
(select Max(b.economic_code_) from MVPN b where b.msisdn = RIGHT(total_EDW_reports.msisdn_nsk,10)), Null
) as economic_code_n
,  
COALESCE (
(select Max(b.profile_id) from MVPN b where b.msisdn = RIGHT(total_EDW_reports.msisdn_nsk,10)), Null
) as profile_id, kit_number_v

, profile_manage, actual_apn_v,apn_status,ip_address_v,short_code,mvpn_status
from total_EDW_reports
where total_EDW_reports.contract_type_v is null

UNION
select * 
from total_EDW_reports
where total_EDW_reports.contract_type_v is not null
"""

total_EDW_reports = spark.sql(query4)

# Convert spark DataFrame to PyArrow Table
pandas_df = total_EDW_reports.toPandas()
table = pa.Table.from_pandas(pandas_df)

# Write the PyArrow Table to Parquet format
pq.write_table(table, root+"total_EDW_reportsFinal.parquet2")

# Write the PyArrow Table to a CSV file
with open(root+"total_EDW_reportsFinal.csv", "wb") as f:
    csv.write_csv(table, f)

# Define and create a temporary view for each DataFrame
total_EDW_reports.createOrReplaceTempView("total_EDW_reports")

# Read final total_EDW_reports
finalS = spark.read.parquet(root+"total_EDW_reportsFinal.parquet2")
filtered_df = finalS.filter((col("msisdn_nsk") == "989304273240") | (col("actual_apn_v").like("SAYANCARDPOS")))
filtered_df.show()

# Print total_EDW_reports schema
total_EDW_reports.printSchema()

# Stop the Spark session
spark.stop()

# Read the Parquet file into a PyArrow Table
table = pq.read_table(root+"total_EDW_reportsFinal.parquet2")
# Convert the PyArrow Table to a Pandas DataFrame
pandasDF = table.to_pandas()
# Filter rows where multiple columns meet conditions
msisdn_nsk = '989304273240'
actual_apn_v = 'SAYANCARDPOS'
economic_code_n = '10861677542'
kit_number_v = ''   

#filtered_pandasDF = pandasDF[(pandasDF['msisdn_nsk'] == '989304273240') | (pandasDF['actual_apn_v'] == 'SAYANCARDPOS')
#                             | (pandasDF['economic_code_n'] == '10861677542') | (pandasDF['kit_number_v'] == '')]

filtered_pandasDF = pandasDF[(pandasDF['msisdn_nsk'] == msisdn_nsk) | (pandasDF['actual_apn_v'] == actual_apn_v)
                             | (pandasDF['economic_code_n'] == economic_code_n) | (pandasDF['kit_number_v'] == kit_number_v)]
print(filtered_pandasDF)

