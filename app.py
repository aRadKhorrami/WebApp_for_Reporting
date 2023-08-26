from flask import Flask, render_template, jsonify, request, Response, session, redirect, url_for
from io import BytesIO  # Import BytesIO
import pyodbc
import pandas as pd
import xlsxwriter


app = Flask(__name__, static_folder='static')
app.secret_key = 'your_secret_key'  # Set a secret key for session management
# Fake user authentication for demonstration purposes
authenticated_users = set()

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
            return redirect(url_for('download_excel'))

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
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()

        query = "select * FROM [sahar].[dbo].[EDWEB_DAY_MVPN_PREPAID_20230810_20230811]"
        df = pd.read_sql(query, cnxn)
        # Create a Pandas DataFrame from the query result
    #   df = pd.DataFrame(query_result)

        cursor.close()
        cnxn.close()     

        # Create an Excel writer
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')

        # Write the DataFrame to the Excel writer
        df.to_excel(writer, sheet_name='Sheet1', index=False)

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
    #check if last session_id works
    server = '127.0.0.1'; database = 'sahar'; username = 'flask'; password = '123'  
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    query = " select USER FROM [dbo].[CREDENTIALS] where [USER] ='{}' ".format(user_) +" and [PASS]='{}' ;".format(pass_)
    Result = pd.read_sql(query, cnxn)
    cursor.close()
    cnxn.close()     
    if (Result.size>0):
        return True
    else:
        return False



if __name__ == '__main__':
    app.run(debug=True)
