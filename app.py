from unittest import result
from flask import Flask, render_template, jsonify, request, Response, session, redirect, url_for
from io import BytesIO  # Import BytesIO
import pyodbc
import pandas as pd
#import xlsxwriter

Result = pd.DataFrame()


app = Flask(__name__, static_folder='static')
app.secret_key = 'your_secret_key'  # Set a secret key for session management
# Fake user authentication for demonstration purposes
authenticated_users = set()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    global user_
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


@app.route('/submit', methods=['GET', 'POST'])
def submit():
    if request.method == 'POST':    
        data = request.get_json()

        msisdn_nsk = data['msisdn_nsk']
        actual_apn_v = data['actual_apn_v']
        economic_code_n = data['economic_code_n']
        kit_number_v = data['kit_number_v']                
        
        # Do something with the recieved parameters
        print(f"Received submit attempt with msisdn_nsk '{msisdn_nsk}', actual_apn_v '{actual_apn_v}', economic_code_n '{economic_code_n}', and kit_number_v '{kit_number_v}'")

#        server = '127.0.0.1'; database = 'sahar'; username = 'flask'; password = '123'  
#        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cnxn = pyodbc.connect('Driver={SQL Server};'
                          'Server=tew116;'
                          'Database=EB_DB;'
                          'Trusted_Connection=yes;')        
        cursor = cnxn.cursor()
        
        authorized_query = generate_authorized_query(msisdn_nsk, actual_apn_v, economic_code_n, kit_number_v, user_)
        report_query = generate_report_query(msisdn_nsk, actual_apn_v, economic_code_n, kit_number_v)
        print(report_query)

        global Result
        Result = pd.read_sql(report_query, cnxn)
        Authorized = pd.read_sql(authorized_query, cnxn)
        cursor.close()
        cnxn.close() 

        if (Authorized.size>0):
            if (Result.size>0):
                return jsonify({'message': f"Some records were found!"})
            else:
                return jsonify({'message': f"There was no record!"}) 
        else:
            return jsonify({'message': f"You don't have sufficient permission to see the results!"})



@app.route('/download_excel', methods=['GET'])
def download_excel():
    if 'username' in session and session['username'] in authenticated_users:
        print("'username' in session and session['username'] in authenticated_users")
        
        # Create an Excel writer
#        output = BytesIO()
#        writer = pd.ExcelWriter(output, engine='xlsxwriter')

        # Write the DataFrame to the Excel writer
        #df.to_excel(writer, sheet_name='Sheet1', index=False)
        global Result
#        Result.to_excel(writer, sheet_name='Sheet1', index=False)

        # Save the Excel writer to the BytesIO object
#        writer.close()
#        output.seek(0)

        # Create a CSV string
        csv_data = Result.to_csv(index=False, encoding='utf-8-sig')

        # Convert the string to bytes
        csv_bytes = csv_data.encode('utf-8-sig')        

        # Create a Flask response with the Excel data
#        response = Response(output.read())
        # Create a Flask response with the CSV data
        response = Response(csv_bytes)

#        response.headers['Content-Disposition'] = 'attachment; filename=query_result.xlsx'
#        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

        response.headers['Content-Disposition'] = 'attachment; filename=query_result.csv'
        response.headers['Content-Type'] = 'text/csv'

        return response
    else:
        print("'username' is not in session or session['username'] is not in authenticated_users")
        return 'Access denied'

def authenticate_user(user_, pass_):
    #check if last session_id works
#    server = '127.0.0.1'; database = 'sahar'; username = 'flask'; password = '123'  
#    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cnxn = pyodbc.connect('Driver={SQL Server};'
                          'Server=tew116;'
                          'Database=EB_DB;'
                          'Trusted_Connection=yes;')       
    cursor = cnxn.cursor()
    query = " select USER FROM [dbo].[CREDENTIALS] where [USER] ='{}' ".format(user_) +" and [PASS]='{}' ;".format(pass_)
    Result = pd.read_sql(query, cnxn)
    cursor.close()
    cnxn.close()     
    if (Result.size>0):
        return True
    else:
        return False

def generate_report_query(msisdn_nsk, actual_apn_v, economic_code_n, kit_number_v):
    # Initialize the base query
    query = "select * FROM [dbo].[total_EDW_reports] WHERE "

    # Create conditions for non-null values
    conditions = []
    if msisdn_nsk is not '':
        conditions.append(f"msisdn_nsk = '{msisdn_nsk}'")
    if actual_apn_v is not '':
        conditions.append(f"actual_apn_v = '{actual_apn_v}'")
    if economic_code_n is not '':
        conditions.append(f"economic_code_n = '{economic_code_n}'")
    if kit_number_v is not '':
        conditions.append(f"kit_number_v = '{kit_number_v}'")

    # Concatenate conditions with 'AND' operator
    query += " AND ".join(conditions)

    return query


def generate_authorized_query(msisdn_nsk, actual_apn_v, economic_code_n, kit_number_v, user_):
    # Initialize the base query
    query = "select * FROM [sahar].[dbo].[total_EDW_reports] WHERE "

    # Create conditions for non-null values
    conditions = []
    if msisdn_nsk is not '':
        conditions.append(f"msisdn_nsk = '{msisdn_nsk}'")
    if actual_apn_v is not '':
        conditions.append(f"actual_apn_v = '{actual_apn_v}'")
    if economic_code_n is not '':
        conditions.append(f"economic_code_n = '{economic_code_n}'")
    if kit_number_v is not '':
        conditions.append(f"kit_number_v = '{kit_number_v}'")

    # Concatenate conditions with 'AND' operator
    query += " AND ".join(conditions)
    query += " and  [economic_code_n] IN \
                (select [economic_code_n] from [dbo].[AUTHENTICATION] where [USER] ='{}' ".format(user_) +");"
    return query


if __name__ == '__main__':
    app.run(debug=True)
