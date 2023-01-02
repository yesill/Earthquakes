import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import sqlite3

class Earthquakes():
    """
    requirements;
    import requests
    from bs4 import BeautifulSoup
    import numpy as np
    import pandas as pd
    import sqlite3
    """   
    
    __url = None
    __db_path = None
    
    def __init__(self, url, db_path):
        """
        Expects 2 parameters.
        url: Url of the official site of 'Kandilli Rasathanesi Son Depremler'.
        db_path: Path of the SQLite Database.
        !!! DATABASEM MUST BE SQLITE ONLY! !!!
        """
        try:
            self.setUrl(url_string=url)
        except:
            print("ERROR: Error while setting url.")
        try:    
            self.setDBPath(db_path=db_path)
        except:
            print("ERROR: Error while setting database path.")
    
    #### functions ####
    # setter - getter #
    def setUrl(self, url_string: str):
        """
        Change url. Url must be string.
        """
        self.__url = url_string
    
    def setDBPath(self, db_path: str):
        """
        Change database path. Database path must be string.
        """
        self.__db_path = db_path
        
    def info(self):
        """
        Returns url and database path as string.
        """
        return self.__url, self.__db_path
    
    # get data from we site #    
    def getSiteSoup(self):
        """
        Returns BeautifulSoup soup object of the web site.
        """
        try:
            if (self.__url):
                request = requests.get(self.__url)
            else:
                print("ERROR: Error in URL. Check URL using info (.info()) function")
        except:
            print("ERROR: Error while requesting url. Check internet connection and url. ( .info() )")
        return BeautifulSoup(request.text, 'html.parser')
        
    def getRawRows(self, soup) -> list:
        """
        Expects a Beautifulsoup soup object as parameter.
        Returns a list object that contains raw rows as string for each row.
        """
        try:
            raw_rows = str(soup.find("body",{"bgcolor":"Ivory"}).find("pre"))
            raw_rows = raw_rows.split("\r\n")
        except:
            print("ERROR: Error while parsing the soup.")
        return raw_rows[7:-2]
    
    def getLocations(self, raw_rows) -> list:
        """
        Expects a list object that contains raw rows.
        Returns a list object that contains cleared locoation info only!
        """
        try:
            locations = []
            for i in raw_rows:
                temp_list = i.split()
                location = temp_list[8:-1]
                if "REVIZE01" in location:
                    location = location[:-2]
                locations.append(" ".join(location))
            return locations
        except:
            print("ERROR: Error while arranging locations.")
    
    def createDataFrame(self, raw_rows, lang="en") -> pd.DataFrame:
        """
        Expects a list object that contains raw rows.
        Lang parameter is changes column names of dataframe.
        It is set to be "en" as default. There is two options for lang parameter. It can be either "en" or "tr". 
        Returns a pandas DataFrame object.
        """
        try:
            locations = self.getLocations(raw_rows=raw_rows)
            cleared_list = []
            for i in range(len(raw_rows)):
                temp_list = raw_rows[i].split()
                temp_list = temp_list[0:8]
                temp_list.append(locations[i])
                cleared_list.append(temp_list)
            columns_tr = ["Tarih","Saat","Enlem(N)","Boylam(E)","Derinlik(km)","MD","ML","Mw","Konum"]
            columns_en = ["Date","Hour","Latitude(N)","Longtitude(E)","Depth","MD","ML","Mw","Location"]
            return pd.DataFrame(cleared_list, columns=columns_tr if lang == "tr" else columns_en)
        except:
            print("ERROR: Error while creating DataFrame.")
        
    def readFromSqlite(self, query='SELECT * FROM EARTHQUAKES', db_path="self") -> pd.DataFrame:
        """
        Query is a SQLite query. It can be changed according to need.
        It is set to be 'SELECT * FROM EARTHQUAKES' as default.
        Returns a pandas DataFrame object which readed from given db_path while initialization.
        db_path is file path of the SQLite database which dataframe will be readad and saved in.
        db_path set to be db_path by default which has given while class initialization.
        It can be changed according to need.
        """
        try:
            cnx = sqlite3.connect(self.__db_path if db_path == "self" else db_path)
            data_frame = pd.DataFrame()
            data_frame = pd.read_sql(query, con=cnx)
            cnx.close()
            data_frame.drop(['index'], axis = 1, inplace = True)
            return data_frame
        except:
            print("ERROR: Error while reading from SQLite database.")
    
    def writeToSqlite(self, data_frame, db_path="self"):
        """
        Expects a pandas DataFrame object to save it as SQLite table.
        db_path is file path of the SQLite database which dataframe will be saved in.
        db_path set to be db_path by default which has given while class initialization.
        It can be changed according to need.
        """
        try:
            cnx = sqlite3.connect(self.__db_path if db_path == "self" else db_path)
            table_name = "EARTHQUAKES"
            cnx.execute(f'DROP TABLE IF EXISTS {table_name}')
            data_frame.to_sql(table_name, con=cnx)
            cnx.close()
            print("DataFrame successfully saved as SQLite table.")
        except:
            print("ERROR: Error while writing dataframe to SQLite database.")
            
    def updateSQLite(self, data_frame, db_path="self"):
        """
        Expects a pandas DataFrame object to save it as SQLite table.
        This function updates the existing SQLite table. It's doing so by deleting existing table
        and writing updated dataframe with same table name as deleted one.
        db_path is file path of the SQLite database which dataframe will be readad and saved in.
        db_path set to be db_path by default which has given while class initialization.
        It can be changed according to need.
        """
        try:
            readed_df = self.readFromSqlite(query='SELECT * FROM EARTHQUAKES',
                                            db_path=self.__db_path if db_path=="self" else db_path)
        except:
            print("ERROR: Error while reading from database.")
        difference_df = pd.concat([data_frame,readed_df]).drop_duplicates(keep=False)
        difference_df[0:len(difference_df.index)//2]
        sql_df = pd.concat([difference_df[0:len(difference_df.index)//2],readed_df])
        try:
            self.writeToSqlite(sql_df)
        except:
            print("ERROR: Error while writing dataframe to SQLite database.")
    
    def getDataFrameQuick(self):
        """
        Returns a pandas DataFrame object which contains last 500 earthquakes data.
        """
        soup = self.getSiteSoup()
        raw_rows = self.getRawRows(soup)
        df = self.createDataFrame(raw_rows)
        return df