import os.path, time
from dateutil import parser
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

root = "E:/MyGitProjects/myFlaskProjects/WebApp_for_Reporting/keyboard700tomani/"
MaxModifiedTime = time.ctime(max(os.path.getmtime(root+z) for z in os.listdir(root)))
MaxModifiedTime = parser.parse(MaxModifiedTime)
print (MaxModifiedTime)

for z in os.listdir(root):
    TT = time.ctime(os.path.getmtime(root+z))
    TT = parser.parse(TT)
    if TT==MaxModifiedTime:
        LFile=root+z
        print(LFile)

#LFile = "H:/CDR Collection/Base_Info/NEID/test.csv"
data = pd.read_csv(LFile,encoding = 'utf-8',error_bad_lines=False ) 


# Path to the output Parquet file
parquet_file_path ='E:/output.parquet2'

# Convert pandas DataFrame to PyArrow Table
table = pa.Table.from_pandas(data)

# Write the PyArrow Table to Parquet format
pq.write_table(table, parquet_file_path)

# Read the Parquet file and print its content
parquet_table = pq.read_table(parquet_file_path)
print(parquet_table.to_pandas())