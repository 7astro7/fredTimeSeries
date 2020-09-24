
from fredapi import Fred
import pandas as pd
import psycopg2
import psycopg2.extras as extras 
import os

class fredDBTable(object):

    def __init__(self, seriesName = None, database = 'fred', api_key = None):
        self.seriesName = seriesName
        self.checkSeriesNameExists()
        self.database = database
        if not api_key:
            self.fred = Fred(api_key = os.environ.get("FRED_API_KEY"))
        self.configs = {
            'host': os.environ.get('PGHOST'),
            'database': self.database,  
            'user': os.environ.get('PGUSER'), 
            'password': os.environ.get('PGPW'), 
            'port': os.environ.get('PGPORT')}
        self.PK = "timepoint_id"
        self.dateColumn = 'date_of_obs'
        self.__createTableSyntax = [self.PK + ' SERIAL PRIMARY KEY ', 
            self.dateColumn + ' DATE']
        self.tableCreationCommandsString = None
        self.hashMap = {
            'info': None,
            'series': self.seriesName, 
            'df': None,
            'tableName': None}

    def checkSeriesNameExists(self):
        """ 
        Tests that a series name has been passed to the constructor. 
        """
        e = """
            Series name has not been assigned.
            Provide series name to constructer
            """
        if not self.seriesName:
            raise Exception(e)

    def getHashMap(self):
        return self.hashMap
    
    def setHashMap(self):
        """ 
        Creates a dictionary via query of FRED using fredapi that 
        stores series info, series name (short version of long 
        official series title), dataframe representation of series,
        table name (official long series name) that will be used to name 
        the table in the PostgreSQL db. 
        """
        # retrieves series information from FRED using fredapi
        vInfo = self.fred.get_series_info(self.seriesName)

        # retrieves the vector using fredapi 
        vSeries = self.fred.get_series(self.seriesName)

        # the series is now stored as a dataframe
        vDF = pd.DataFrame(vSeries)

        # a date column is created from the date values that 
        # are originally stored in index 
        vDF[self.dateColumn] = pd.to_datetime(vDF.index)

        # table name is constructed from official, long series 
        # name and whitespace is replaced with underscores
        tableName = vInfo.title.replace(" ", "_").lower()

        # a new column is assigned using short name of the series, 
        # made all lowercase. the unnamed series at column 0 will
        # is dropped 
        vDF[vInfo.id.lower()] = vDF[0]
        vDF = vDF.drop(columns = 0)

        # df index is changed from storing date values to regular 
        # integers
        vDF = vDF.reset_index(drop = True)
        
        # if a character in the table name (for db) is not underscore
        # or alphabetic it's replaced with an underscore
        for i in range(len(tableName)):
            if not (tableName[i] == "_" or tableName[i].isalpha()):
                tableName = tableName[:i] + "_" + tableName[i + 1:]
        
        # hashMap metadata values are assigned 
        self.hashMap['info'], self.hashMap['df'] = vInfo, vDF
        self.hashMap['tableName'] = tableName

    def toCSV(self, path = None):
        """ 
        Exports series stored in dataframe to csv for testing, 
        etc. """
        # This method is provided only for flexibility 
        self.checkSeriesNameExists()
        try:
            if not path:
                self.hashMap['df'].to_csv(self.hashMap['tableName'] + ".csv")
            else:
                self.hashMap['df'].to_csv(path + self.hashMap['df'] + ".csv")
        except (Exception) as e:
            print('CSV was not created: ', e)

    def __generateCreateTableCommands(self):
        """ Creates SQL syntax to create db table with the long, 
        official FRED name of the series used as the table name. 
        """
        try:
            if not len(self.hashMap['df']):
                self.constructHashMap()
        except:
            raise Exception("Syntax to create table not generated")
        self.__createTableSyntax.append(self.hashMap['df'].columns[1] + " NUMERIC")
        prefix = " CREATE TABLE " + self.hashMap['tableName'] + " ( "
        suffix, c = ");", ""
        for i in range(len(self.__createTableSyntax)):
            if i == len(self.__createTableSyntax) - 1:
                c += self.__createTableSyntax[i] 
            else:
                c += self.__createTableSyntax[i] + ", " 
        self.tableCreationCommandsString = prefix + c + suffix
   
    def createTable(self): 
        """ 
        Creates table in PostgreSQL db, without any insertion of 
        data, to store the series. createTable() uses the official, 
        long series name as assigned in FRED as name of relation
        """
        conn = None 
        try: 
            if not self.tableCreationCommandsString:
                self.__generateCreateTableCommands()
            print('\nConnecting to the PostgreSQL database...')
            conn = psycopg2.connect(**self.configs)
            cur = conn.cursor()
            cur.execute(self.tableCreationCommandsString)
            cur.close()
            conn.commit()
            print("Successful commit, table is created")
        except (Exception, psycopg2.DatabaseError) as e:
            print("Error: %s" % e)
        finally:
            if conn is not None:
                conn.close()
        print("Connection is closed")

    def populateTable(self):
        """
        Inserts all rows present in series into newly created 
        table in PostgreSQL. if createTable() hasn't been called
        and therefore no table exists in the database when 
        populateTable() is called, the table will be created provided
        a seriesName is given
        """
        # ensures that the hashMap has been assigned the data 
        # needed to create the db table
        if not self.hashMap['df']:
            self.setHashMap()
        self.createTable()

        # list of tuples containing rows to insert is made from 
        # dataframe values. primary key value assignments are automatic
        # and not dealt with here
        rowsToInsert = [(self.hashMap['df'].iloc[i, 0], 
            self.hashMap['df'].iloc[i, 1]) 
            for i in range(len(self.hashMap['df']))]

        # SQL query to execute is generated and stored in query
        query = "INSERT INTO %s(%s) VALUES(%%s, %%s)" % (self.hashMap['tableName'], 
        ", ".join(list(self.hashMap['df'].columns)))
        # inserts the tuples that are stored in rowsToInsert into the 
        # table using execute_batch() method. exit code of 1 denotes 
        # failed attempt to complete this bulk insert
        conn = None
        try:
            conn = psycopg2.connect(**self.configs)
            cur = conn.cursor()
            extras.execute_batch(cur, query, rowsToInsert)
            conn.commit()
            print("Insertion complete")
        except (Exception, psycopg2.DatabaseError) as e:
            print("Error: %s" % e)
        finally:
            if conn is not None:
                conn.close()
        return self.hashMap

#db = fredDBTable("C309RA3A086NBEA").populateTable()


