"""
this module will help to connect the database and write and read from database
"""

import psycopg2
import psycopg2.extras
import pandas as pd

import re
from datetime import datetime, timedelta


class Database:
    def __init__(self, password='', user='',
                 host='', port='5432',
                 database=''):
        """
        initialize the parameters
        :param host: database host
        :param port: database port
        :param database: name of the database
        :param user: username of database
        :param password: password for the user
        """
        self.host = host
        self.port = port
        self.databaseName = database
        self.user = user
        self.password = password
        self.connState = False
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.databaseName,
                user=self.user,
                password=self.password
            )
            self.curr = self.conn.cursor()
            self.connState = True
        except:
            print("Error while connecting to the database")

    def execute(self, sqlQuery):
        """
        function will execute the query on the connection
        :param sqlQuery:
        :return: data
        """
        if self.connState:
            try:
                self.curr.execute(str(sqlQuery))
                x = self.curr.fetchone()
                return x
            except Exception as err:
                print("error while executing query: {}".format(err))
        else:
            print("Need to Connect DB")

    def readDataFromDBToPd(self, sql):
        """
        it will read data from table to pandas dataframe
        :param sql: squery to read
        :return: pandas dataframe
        """
        if self.connState:
            return pd.read_sql_query(sql, self.conn)
        else:
            print("error to read data from db2pd table to pandas dataframe")

    def readTableToPD(self, tableName, limit='na'):
        """
        this will read entire table to pandas dataframe
        :param tableName:
        :return:
        """
        if limit == 'na':
            sqlQuery = "select * from {}".format(tableName)
        else:
            sqlQuery = "select * from {} limit {}".format(tableName, limit)

        if self.connState:
            return self.readDataFromDBToPd(sqlQuery)
        else:
            print("error to read data from db2pd table to pandas dataframe")

    def writeInsertDataFromPDToDB(self, tableName, dataFrame):
        """
        this function will write dataframe to the postgress database table
        table name : name for pandas data frame is same.
        :param dataFrame: pandas dataframe which need to save to db2pd
        :param tableName: Name of the table at which we need to write
        :return: True if successful else False
        """

        if len(dataFrame) > 0 and self.connState:
            dfColumns = list(dataFrame)
            columns = ",".join(dfColumns)
            vals = "VALUES({})".format(",".join(["%s" for _ in dfColumns]))
            insertStmt = "INSERT INTO {}({}) {}".format(tableName, columns, vals)
            try:
                psycopg2.extras.execute_batch(self.curr, insertStmt, dataFrame.values)
                self.conn.commit()
                return True
            except Exception as exp:
                print("error while executing batch insert of DF {}".format(exp))
                return False

    def writeUpdateDataFromPDToDB(self, tableName, dataFrame, condition):
        """
        this function will write dataframe to the postgress database table
        :param dataFrame: pandas dataframe which need to save to db2pd
        :param tableName: Name of the table at which we need to write
        :return: True if successful else False
        """

        if len(dataFrame) > 0 and self.connState:
            dfColumns = list(dataFrame)
            columns = ",".join(dfColumns)
            # vals = "SET ({})".format("({}) = %({})s".join([cols for cols in list(dataFrame.columns)]))
            # org outPut = "SET {} ".format(",".join(["{}=%({})s".format(i, i) for i in list(dataFrame.columns)]))
            set = "SET {} ".format(",".join(["{}= dt.{}".format(i, i) for i in list(dataFrame.columns)]))
            condX, condY = condition.split("==")
            condition = "WHERE t.{} = dt.{}".format(condX, condY)
            vals = " FROM (VALUES %s)"  # .format(",".join(["%s" for _ in dfColumns]))
            frm = "AS dt({})".format(", ".join(["{}".format(col) for col in dfColumns]))
            from_stmt = vals + " " + frm
            output = set + from_stmt + condition
            insertStmt = "UPDATE {} AS t {}".format(tableName, output)
            # print(insertStmt)
            # cols = ",".join(['{}{}{}'.format('"', col, '"') for col in dfColumns])
            # cols = cols.replace("'", "")
            # tuples = [tuple(x) for x in dataFrame.values]
            # print(tuples[0])
            try:
                psycopg2.extras.execute_values(self.curr, insertStmt, dataFrame.values, template=None, page_size=100)
                self.conn.commit()
                return True
            except Exception as exp:
                print("error while executing batch update of DF {}".format(exp))
                return False

    def single_insert(self, insert_req):
        """
        this function is supporting function will help to write daframe to database
        :param insert_req:
        :return:
        """
        try:
            self.curr.execute(insert_req)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()

    ## make function for table truncation
    def truncateTable(self, tableName):
        """
        this function will truncate the table
        :param tableName: Name of the table to be truncated
        :return:
        """
        try:
            SQL = "TRUNCATE TABLE {}".format(tableName)
            self.curr.execute(str(SQL))
            self.conn.commit()
            return True
        except Exception as exp:
            print("error while truncating {}".format(exp))
            return False

    def deleteDuplicates(self, tableName, conditionColumn):
        """
        this function will delete entries in the table based on the condition
        :param tableName: Name of table with schema
        :param conditionColumn:
        :return:
        """
        sql = "DELETE FROM {} WHERE {} NOT IN ( SELECT distinct {} FROM {})".format(tableName, conditionColumn,
                                                                                      conditionColumn, tableName)
        try:
            self.curr.execute(str(sql))
            self.conn.commit()
            return True
        except Exception as exp:
            print("error while Deleting Duplicates {}".format(exp))
            return False

    def closeConn(self):
        """
        this will close the connection
        :return: None
        """
        self.curr.close()
        self.conn.close()
        self.connState = False

    def getColumnValues(self, tableName, selectColumnsName, startDate, endDate='na', whereColumnName='load_crt_dt'):
        """
        this function will return a column values. more specifically i have used this function
        to get post Id's for updating comments i.e incremental update
        :param tableName: Name of the table
        :param columnsName: Name of column which you want to have
        :param startDate: from which date
        :param endDate: upto which date
        :return: it return dataframe containing post id's
        """
        start = datetime.strptime(startDate, "%d/%m/%y") - timedelta(days=7)
        endDate = start - timedelta(days=28)
        start = start.date()
        endDate = endDate.date()
        sql = "select {} from {} where {} between date('{}') and date('{}')".format(selectColumnsName, tableName,
                                                                                    whereColumnName, str(endDate),
                                                                                    str(start))
        # print(sql)
        if self.connState:
            return self.readDataFromDBToPd(sql)
