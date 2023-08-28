import os.path, time
from dateutil import parser
import pandas as pd 
import pyodbc
while True:
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
    #data = pd.read_csv(LFile,error_bad_lines=False )

    data['mesg_date_d'] = data['mesg_date_d'].fillna('')
    data['profile_id'] = data['profile_id'].fillna(0)
    data['serv_user_name_v'] = data['serv_user_name_v'].fillna(0)
    data['company_name'] = data['company_name'].fillna(0)
    data['acc_link_code'] = data['acc_link_code'].fillna(0)
    data['short_code'] = data['short_code'].fillna(0)
    data['group_code_n'] = data['group_code_n'].fillna(0)
    data['group_name_v'] = data['group_name_v'].fillna(0)
    data['mvpn_status'] = data['mvpn_status'].fillna(0)
    data['ability_status'] = data['ability_status'].fillna(0)
    data['msisdn'] = data['msisdn'].fillna(0)
    data['package_code_v'] = data['package_code_v'].fillna(0)
    data['contract_type_v'] = data['contract_type_v'].fillna(0)
    data['registration_date_d'] = data['registration_date_d'].fillna(0)
    data['ability_activation_date'] = data['ability_activation_date'].fillna(0)
    data['mvpn_activation_date_d'] = data['mvpn_activation_date_d'].fillna(0)
    data['mvpn_status_change_date'] = data['mvpn_status_change_date'].fillna(0)
    data['flat_flag'] = data['flat_flag'].fillna(0)
    data['profile_creation_date'] = data['profile_creation_date'].fillna(0)
    data['economic_code_'] = data['economic_code_'].fillna(0)
#    data['FileDate'] = data['FileDate'].fillna(0)

    data.head(10)

    server = '127.0.0.1'; database = 'sahar'; username = 'flask'; password = '123'; charset = 'utf8'; use_unicode = 'True'
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password + ';CHARSET=' + charset + ';USE_UNICODE=' + use_unicode)
    cursor = cnxn.cursor()

    SQLCommand = ("truncate table sahar.dbo.Temp_EDWEB_DAY_MVPN_PREPAID;")
    cursor.execute(SQLCommand)
    cnxn.commit()

    for index, row in data.iterrows():    
        mesg_date_d = row['mesg_date_d'];
        profile_id = row['profile_id'];
        serv_user_name_v = row['serv_user_name_v'];
        company_name = row['company_name'];
        acc_link_code = row['acc_link_code'];
        short_code = row['short_code'];
        group_code_n = row['group_code_n'];
        group_name_v = row['group_name_v'];
        mvpn_status = row['mvpn_status'];
        ability_status = row['ability_status'];
        msisdn = row['msisdn'];
        package_code_v = row['package_code_v'];
        contract_type_v = row['contract_type_v'];
        registration_date_d = row['registration_date_d'];
        ability_activation_date = row['ability_activation_date'];
        mvpn_activation_date_d = row['mvpn_activation_date_d'];
        mvpn_status_change_date = row['mvpn_status_change_date'];
        flat_flag = row['flat_flag'];
        profile_creation_date = row['profile_creation_date'];
        economic_code_ = row['economic_code_'];
    #    FileDate = row['FileDate'];


        SQLCommand = ("INSERT INTO [dbo].Temp_EDWEB_DAY_MVPN_PREPAID ([mesg_date_d],[profile_id],[serv_user_name_v],[company_name],[acc_link_code]\
                ,[short_code],[group_code_n],[group_name_v],[mvpn_status],[ability_status],[msisdn],[package_code_v],[contract_type_v]\
                ,[registration_date_d],[ability_activation_date],[mvpn_activation_date_d],[mvpn_status_change_date],[flat_flag],[profile_creation_date]\
                ,[economic_code_],[FileDate]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
#        try:
        cursor.execute(SQLCommand,mesg_date_d,profile_id,serv_user_name_v,company_name,acc_link_code,short_code,group_code_n,group_name_v,mvpn_status
                ,ability_status,msisdn,package_code_v,contract_type_v,registration_date_d,ability_activation_date,mvpn_activation_date_d
                ,mvpn_status_change_date,flat_flag,profile_creation_date,economic_code_,'20230810_20230811')
#        except:
#            pass
        cnxn.commit()
    '''
    SQLCommand = ("insert into dbo.NetworkElement \
    select NetName,Name,CityName,TypeName,NEID,Null as NEType,NEFactory,OPrefix, Null as LastFile, Null as LastTry, Null as LastModified from FTP.dbo.TempNetElements T \
    where T.NEID not in (select NEID from dbo.NetworkElement);")

    try:
        cursor.execute(SQLCommand)
    except:
        print('error')
        pass
    cnxn.commit()
    
    SQLCommand = ("truncate table FTP.dbo.TempNetElements;")
    cursor.execute(SQLCommand)
    cnxn.commit()
    '''
    cursor.close()
    cnxn.close()    

    print("end")
#    time.sleep(86400)
    time.sleep(600)

