from flask import Flask, render_template, jsonify, request, Response, session, redirect, url_for
from io import BytesIO  # Import BytesIO
#import pyodbc
import sqlite3
import pandas as pd
import xlsxwriter
#import pyarrow.parquet as pq
import configparser
import os.path

app = Flask(__name__, static_folder='static')
app.secret_key = 'your_secret_key'  # Set a secret key for session management
# Fake user authentication for demonstration purposes
authenticated_users = set()


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

# Read the Parquet file into a PyArrow Table
#table = pq.read_table(root+"total_EDW_reportsFinal.parquet2")
# Convert the PyArrow Table to a Pandas DataFrame
#pandasDF = table.to_pandas()

pandasDF = pd.read_csv(root+"total_EDW_reportsFinal.csv")

#pandasDF.info()
msisdn_nsk = ''
actual_apn_v = ''
economic_code_n = ''
kit_number_v = ''      


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':    
        data = request.get_json()
        user_ = data['username']
        pass_ = data['password']
        
        # Do something with the username and password
        print(f"Received login attempt with username '{user_}' and password '{pass_}'")

        # Perform user authentication (replace with your own authentication logic)
        if authenticate_user(user_, pass_):
            authenticated_users.add(user_)
            session['username'] = user_
            return jsonify({'message': f"Successful Login!"})
        else:
            return jsonify({'message': f"Wrong Username or Password!"})  

#            return redirect(url_for('download_excel'))

        #check if last session_id works
        server = '127.0.0.1'; database = 'sahar'; username = 'flask'; password = '123'  
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        query = " select USER FROM [dbo].[CREDENTIALS] where [USER] ='{}' ".format(user_) +" and [PASS]='{}' ;".format(pass_)
        Result = pd.read_sql(query, cnxn)
        # create empty table
        table = []
        if (Result.size>0):
            #    return jsonify({'message': f"Login attempt with username '{username}' and password '{password}' was successful!"})
            for i in range(8):
                row = {'col1': f"{pass_}", 'col2': '2', 'col3': '3'}
                table.append(row)
        cursor.close()
        cnxn.close() 

            # return table as json
        return jsonify(table)   



@app.route('/submit', methods=['GET', 'POST'])
def submit():
    if request.method == 'POST':    
        data = request.get_json()

        global msisdn_nsk 
        global actual_apn_v 
        global economic_code_n
        global kit_number_v

        msisdn_nsk = data['msisdn_nsk'].strip("'")
        actual_apn_v = data['actual_apn_v'].strip("'")
        economic_code_n = data['economic_code_n'].strip("'")
        kit_number_v = data['kit_number_v'].strip("'")                
        
        # Do something with the username and password
        print(f"Received submit attempt with msisdn_nsk '{msisdn_nsk}', actual_apn_v '{actual_apn_v}', economic_code_n '{economic_code_n}', and kit_number_v '{kit_number_v}'")

        # Perform user authentication (replace with your own authentication logic)

#        if authenticate_user(user_, pass_):
#            authenticated_users.add(user_)
#            session['username'] = user_
#            return redirect(url_for('download_excel'))

        #check if last session_id works
        server = '127.0.0.1'; database = 'sahar'; username = 'flask'; password = '123'  
#        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
#        cursor = cnxn.cursor()
#        query = " select msisdn_nsk, actual_apn_v, economic_code_n, kit_number_v FROM [dbo].[total_EDW_reports] \
#            where [msisdn_nsk] ='{}' ".format(msisdn_nsk) +" or [actual_apn_v]='{}' ".format(actual_apn_v) +" \
#            or [economic_code_n] ='{}' ".format(economic_code_n) +" or [kit_number_v]='{}' ;".format(kit_number_v)
#        Result = pd.read_sql(query, cnxn)
#        cursor.close()
#        cnxn.close() 
        print(msisdn_nsk)
        global pandasDF
        filtered_pandasDF = pandasDF[(pandasDF['msisdn_nsk'] == int(msisdn_nsk)) | (pandasDF['actual_apn_v'] == actual_apn_v)
                             | (pandasDF['economic_code_n'] == economic_code_n) | (pandasDF['kit_number_v'] == kit_number_v)]
        if (filtered_pandasDF.size>0):
            return jsonify({'message': f"Some records were found!"})
        else:
            return jsonify({'message': f"There was no record!"})  



# Example query result as a list of dictionaries
query_result = [
    {'Name': 'Alice', 'Age': 25},
    {'Name': 'Bob', 'Age': 30},
    {'Name': 'Carol', 'Age': 28}
]



@app.route('/download_excel', methods=['GET'])
def download_excel():
    if 'username' in session and session['username'] in authenticated_users:
        print("'username' in session and session['username'] in authenticated_users")
       
        #check if last session_id works
        server = '127.0.0.1'; database = 'sahar'; username = 'flask'; password = '123'  
#        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
#        cursor = cnxn.cursor()

#        query = "select * FROM [sahar].[dbo].[EDWEB_DAY_MVPN_PREPAID_20230810_20230811]"
#        df = pd.read_sql(query, cnxn)
        # Create a Pandas DataFrame from the query result
    #   df = pd.DataFrame(query_result)

#        cursor.close()
#        cnxn.close()     

        # Create an Excel writer
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')

        global pandasDF
        filtered_pandasDF = pandasDF[(pandasDF['msisdn_nsk'] == int(msisdn_nsk)) | (pandasDF['actual_apn_v'] == actual_apn_v)
                             | (pandasDF['economic_code_n'] == economic_code_n) | (pandasDF['kit_number_v'] == kit_number_v)]


        # Write the DataFrame to the Excel writer
        #df.to_excel(writer, sheet_name='Sheet1', index=False)
        filtered_pandasDF.to_excel(writer, sheet_name='Sheet1', index=False)

        # Save the Excel writer to the BytesIO object
        writer.close()
        output.seek(0)

        # Create a Flask response with the Excel data
        response = Response(output.read())
        response.headers['Content-Disposition'] = 'attachment; filename=query_result.xlsx'
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

        return response
    else:
        print("'username' is not in session or session['username'] is not in authenticated_users")
        return 'Access denied'



def authenticate_user(user_, pass_):

    # Connect to the SQLite database or create a new one
    conn = sqlite3.connect('CREDENTIALS.db')

    # Create a cursor object
    cursor = conn.cursor()

    #check if last session_id works
    query = " select username FROM users where username ='{}' ".format(user_) +" and pass='{}' ;".format(pass_)
    Result = pd.read_sql(query, conn)
    cursor.close()
    conn.close()     
    if (Result.size>0):
        return True
    else:
        return False



if __name__ == '__main__':
    app.run(host='0.0.0.0', port='8080', debug=True)
