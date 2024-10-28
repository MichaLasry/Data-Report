import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import json
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker


class Database:

    def __init__(self):
        with open(r'D:\HULK\builds\Reports\config.json') as config_file:
            config = json.load(config_file)
        db_config = config['db']
        self.connection_string = f'mssql+pymssql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}/{db_config["database"]}'
        self.engine = create_engine(self.connection_string)
        self.Session = sessionmaker(bind=self.engine)
        # self.conn = self.engine.connect()

    def close_connection(self):
        self.engine.dispose()

# get testers name and type
    def get_Testers(self):
        with self.engine.connect() as conn:
            query = text("""SELECT Testers_Types.ID, Testers_Types.Name
                    FROM Testers_Types JOIN Testers ON Testers.Type_ID = Testers_Types.ID
                    WHERE Testers_Types.Name <> 'Reports'
                    AND Testers.Show = 1
                    GROUP BY Testers_Types.ID, Testers_Types.Name
                    ORDER BY Testers_Types.Name""")
            df = pd.read_sql(query, conn)
            testers_dict = df.to_dict(orient='records')
        return testers_dict

    def get_Testers_by_Tester_Type(self, Tester_Type):
        with self.engine.connect() as conn:
            query = text(f"""SELECT ID 
                    FROM Testers
                    WHERE
                    Testers.Type_ID={Tester_Type}""")
            df = pd.read_sql_query(query, conn)
            testers = df.to_dict('list')['ID']
        return testers

    def get_Tests_Name(self, tester_Type, start_date, end_date):
        testers_list = self.get_Testers_by_Tester_Type(tester_Type)
        tester_ID_query = self.get_testers_Type_ID_query(testers_list)
        date_obj = datetime.strptime(start_date, "%d/%m/%Y")
        start_date = date_obj.strftime("%Y/%m/%d")
        date_obj = datetime.strptime(end_date, "%d/%m/%Y")
        end_date = date_obj.strftime("%Y/%m/%d")
        with self.engine.connect() as conn:
            query = text(f"""Select DISTINCT Test_Name 
                    FROM Results_Data 
                    WHERE Collected_Data_ID in
                    (
                    SELECT ID FROM Collected_Data 
                    WHERE ({tester_ID_query})
                    AND Result_Text='Pass'
                    AND [Day] >= '{start_date}' AND [Day] <= '{end_date}'
                    
                    )""")
            df = pd.read_sql_query(query, conn)
            tests_name = [[item.replace("[", "").replace("]", "")] if not isinstance(item, list) else item for item in df.to_dict('list')['Test_Name']]
        return tests_name

    def get_testers_Type_ID_query(self, testers_list):
        if not testers_list:
            tester_ID_query = ""
        else:
            if len(testers_list) == 1:
                tester_ID_query = f"Collected_Data.Tester_ID={testers_list[0]}"
            else:
                tester_ID_query = f"Collected_Data.Tester_ID={testers_list[0]}"
                for tester in testers_list[1:]:
                    tester_ID_query = tester_ID_query + f" OR Collected_Data.Tester_ID={tester}"
        return text(tester_ID_query)

    def get_data_search(self, tester_Type, start_date, end_date, cat_query=""):
        tests_name = self.get_Tests_Name(tester_Type, start_date, end_date)
        tests_name_query = ""
        if tests_name:
            tests_name_query = f"{tests_name}".replace("'", "")
            tests_name_query = f"PIVOT (MAX(Result) FOR Test_Name IN ({tests_name_query[1:-1]})) p"
        testers_list = self.get_Testers_by_Tester_Type(tester_Type)
        tester_ID_query = self.get_testers_Type_ID_query(testers_list)
        date_obj = datetime.strptime(start_date, "%d/%m/%Y")
        start_date = date_obj.strftime("%Y/%m/%d")
        date_obj = datetime.strptime(end_date, "%d/%m/%Y")
        end_date = date_obj.strftime("%Y/%m/%d")
        with self.engine.connect() as conn:
            main_query = text(f"""
                        SELECT  row_number() over (order by Start_Time DESC) as rank, *
                        FROM    (
                                    SELECT Start_Time, Tester_Name, Station, Serial_No, Work_Order,Cat,Revision, Test_Time, Result_Text, Test_Name, Result
                                    FROM    
                        (
                        SELECT Start_Time, Testers.Name AS Tester_Name, Stations.Name AS Station, Collected_Data.Serial_No, Work_Order, Cat_No.Cat_No+' '+Cat_No.Name AS Cat, Revision, round(Collected_Data.Test_Time,2) AS Test_Time,Result_Text ,Test_Name,Result
                        FROM Collected_Data 
                        LEFT JOIN Results_Data ON Collected_Data.ID=Results_Data.Collected_Data_ID
                        JOIN Testers ON Collected_Data.Tester_ID=Testers.ID
                        JOIN Testers_Types ON Testers.Type_ID=Testers_Types.ID
                        LEFT JOIN Stations ON Stations.ID=Station_ID
                        LEFT JOIN Cat_No ON Collected_Data.Cat_No = Cat_No.Cat_No
                        
                        JOIN (
                        SELECT Serial_No, MIN(Start_Time) AS LatestTime
                        FROM Collected_Data 
                        WHERE
                        ({tester_ID_query})
                        {cat_query}
                        AND Type=99  
                        AND [Day] >= '{start_date}' AND [Day] <= '{end_date}'
                        GROUP BY Serial_No
                        ) c ON Collected_Data.Serial_No = c.Serial_No AND Collected_Data.Start_Time = c.LatestTime
                     
                    
                        WHERE 
                        ({tester_ID_query})
                        AND [Day] >= '{start_date}' AND [Day] <= '{end_date}'
                        AND Type=99
                            ) Collected_Data	
                                    
                                ) t
                                {tests_name_query}
                            """)
            # print(main_query)
            result_df = pd.read_sql_query(main_query, conn)
            print("main query finish")
            headers = list(result_df.columns)
        return result_df, headers

    def get_cats_No(self, tester_Type, start_date, end_date):
        testers_list = self.get_Testers_by_Tester_Type(tester_Type)
        tester_ID_query = self.get_testers_Type_ID_query(testers_list)
        date_obj = datetime.strptime(start_date, "%d/%m/%Y")
        start_date = date_obj.strftime("%Y/%m/%d")
        date_obj = datetime.strptime(end_date, "%d/%m/%Y")
        end_date = date_obj.strftime("%Y/%m/%d")
        with self.engine.connect() as conn:
            query = text(f"""
                    SELECT DISTINCT Cat_No FROM Collected_Data 
                    WHERE ({tester_ID_query})
                    AND [Day] >= '{start_date}' AND [Day] <= '{end_date}'
                    """)
            result_df = pd.read_sql_query(query, conn)
            list_cats = result_df.to_dict('list')['Cat_No']
        return list_cats
