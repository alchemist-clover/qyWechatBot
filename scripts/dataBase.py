import sqlite3


from log import sqliteLogger


class syncDataBase():
    def __init__(self, dataBaseFilePath):
        self.dataBaseFilePath = dataBaseFilePath

    def __enter__(self):
        self.connection = sqlite3.connect(self.dataBaseFilePath)
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exceptionType, exceptionValue, exceptionTraceBack):
        self.cursor.close()
        if exceptionType is not None:
            sqliteLogger.error(
                "Database operation error, the error message is:\n{}".format(exceptionValue))
            self.connection.rollback()
            self.connection.close()
            raise RuntimeError(
                "Database operation error, the error message is:\n{}".format(exceptionValue))
        self.connection.commit()
        self.connection.close()
        return True

    def tableNames(self):
        tables = self.queryTable("name", "sqlite_master", "where type='table'")
        tableNamesList = []
        for tableTuple in tables:
            tableName, = tableTuple
            tableNamesList.append(tableName)
        return tuple(tableNamesList)

    def createTable(self, tableName, tableStructure):
        sql = "create table {} ({})"
        self.cursor.execute(sql.format(tableName, tableStructure))

    def queryTable(self, columns, tableName, *conditions):
        sql = "select {} from {}".format(columns, tableName)
        sql = " ".join((sql,) + conditions)
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def deleteRows(self, tableName, *conditions):
        sql = "delete from {}".format(tableName)
        sql = " ".join((sql,) + conditions)
        self.cursor.execute(sql)

    def insertRow(self, tableName, *values):
        valuesPlaceHolder = ""
        for i in range(len(values)):
            if i == 0:
                valuesPlaceHolder += "?"
            else:
                valuesPlaceHolder += ", ?"
        sql = "insert into {} values ({})".format(tableName, valuesPlaceHolder)
        self.cursor.execute(sql, values)

    def updateCol(self, tableName, *conditions, **values):
        valuesPlaceHolder = ""
        valuesTuple = ()
        for i, key in enumerate(values):
            if i == 0:
                valuesPlaceHolder += "{}=(?)".format(key)
            else:
                valuesPlaceHolder += ", {}=(?)".format(key)
            valuesTuple += (values[key],)
        sql = "update {} set {}".format(tableName, valuesPlaceHolder)
        sql = " ".join((sql,) + conditions)
        self.cursor.execute(sql, valuesTuple)
