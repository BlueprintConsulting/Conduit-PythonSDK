import os, logging, requests, urllib.parse, time
#from src.conduit_pkg.conduitEntities import Database, Table, Column, QueryResult

logging.basicConfig(
    format="%(name)s-%(levelname)s-%(asctime)s-%(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



class Database(object):
    def __init__(self, database):
        self.database = database
    def __str__(self):
        return "Database: {}".format(self.database)

class Table(object):
    def __init__(self, table, database, schema, tableType):
        self.table = table
        self.database = database
        self.schema = schema
        self.tableType = tableType
    def __str__(self):
        return "Table: {}, from db: {}, is a part of schema: {}, and is of type: {}".format(
            self.table, self.database, self.schema, self.tableType
        )

class Column(object):
    def __init__(self, name, colType, lengthOpt, scaleOpt, sqlType):
        self.name = name
        self.colType = colType
        self.lengthOpt = lengthOpt
        self.scaleOpt = scaleOpt
        self.sqlType = sqlType
    def __str__(self):
        return "ColumnName: {}, Type: {}, LengthOpt: {}, ScaleOpt: {}, sqlType: {}".format(
            self.name,
            self.colType,
            self.lengthOpt,
            self.scaleOpt,
            self.sqlType
        )

class QueryResult(object):
    def __init__(self, jsonLoad):
        self.json = jsonLoad
        self.parseDict()
    def parseDict(self):
        self.queryId = self.json['queryId']
        self.status = self.json['status']
        self.message = self.json['message']
        self.data = self.json['data']
    def __str__(self):
        return "QueryId: {}, with status: {}, has message: {}, check 'data' attribute for data.".format(
            self.queryId, self.status, self.message
        )


def getAllMetadata():
    raise NotImplementedError("TODO: getAllMetadata not implemented yet.")

def getOnTheWire(endpoint):
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer {}'.format(token())
    }
    url = 'https://{}/query{}'.format(server(), endpoint)
    resp = requests.get(url, headers = headers)
    if resp.status_code == 200:
        data = resp.json()
        if data is None:
            data = ""
        return data
    else:
        logger.error("There was an error calling endpoint {}".format(endpoint))

def cancelQuery(queryObj):
    if queryObj.status != "Running":
        logger.info("Query {} isn't marked as running, won't attempt to cancel.".format(queryObj.queryId))
        return True
    curlstring = 'curl -X GET "https://$CONDUIT_SERVER/query/cancel?queryId={queryId}" -H  "accept: application/json" -H "Authorization: Bearer $CONDUIT_TOKEN"'
    logger.debug("Calling cancelQuery, equivalent curl: {}".format(curlstring))
    data = getOnTheWire("/cancel?queryId={}".format(queryObj.queryId))
    if type(data) == dict:
        if data['isCancelled'] == False:
            logger.info("Query {} isn't cancelled yet...checking again".format(queryObj.queryId))
            time.sleep(2)
            cancelQuery(queryObj)
        elif data['isCancelled'] == True:
            logger.info("Query {} successfully cancelled!!".format(queryObj.queryId))
            return True
        else:
            logger.info("Query in a strange state: {}".format(data['isCancelled']))
            return True


def processQueryResult(dataDict):
    qObj = QueryResult(dataDict)
    if qObj.status == "Finished":
        logger.info(qObj)
    elif qObj.status == "Running":
        logger.info("Query {} is Running".format(qObj.queryId, qObj.status))
        time.sleep(2)
        getQueryResult(qObj.queryId)
    else:
        print("Query {} isn't finished, but in status: {}".format(qObj.queryId, qObj.status))
    return qObj
def getQueryResult(queryId):
    curlstring = 'curl -X GET "https://$CONDUIT_SERVER/query/execute/{queryId}/result" -H  "accept: application/json" -H "Authorization: Bearer $CONDUIT_TOKEN"'
    logger.debug("Calling getQueryResult, equivalent curl: {}".format(curlstring))
    data = getOnTheWire("/execute/{}/result".format(queryId))
    if type(data) == dict:
        processQueryResult(data)

def executeSyncQuery(sqlString):
    sqlEncoded = urllib.parse.quote(sqlString)
    curlstring = 'curl -X GET "https://$CONDUIT_SERVER/query/execute?query=SHOW+*+TABLES" -H  "accept: application/json" -H "Authorization: Bearer $CONDUIT_TOKEN"'
    logger.debug("Calling executeSyncQuery, equivalent curl: {}".format(curlstring))
    data = getOnTheWire("/execute?query={}".format(sqlEncoded))
    if type(data) == dict:
        return processQueryResult(data)

def executeAsyncQuery(sqlString):
    sqlEncoded = urllib.parse.quote(sqlString)
    curlstring = 'curl -X GET "https://$CONDUIT_SERVER/query/execute?query=SHOW+*+TABLES" -H  "accept: application/json" -H "Authorization: Bearer $CONDUIT_TOKEN"'
    logger.debug("Calling executeSyncQuery, equivalent curl: {}".format(curlstring))
    data = getOnTheWire("/execute?query={}".format(sqlEncoded))
    if type(data) == dict:
        qObj = QueryResult(data)
        if qObj.status == "Finished":
            print("Query {} was executed async, but already finished".format(qObj.queryId))
        elif qObj.status == "Running":
            print("Query {} is Running".format(qObj.queryId, qObj.status))
        else:
            print("Query {} isn't finished, but in status: {}".format(qObj.queryId, qObj.status))
        return qObj

def getTables(database):
    curlstring = 'curl -X GET "https://$CONDUIT_SERVER/query/metadata/databases/{database}/tables" -H  "accept: application/json" -H "Authorization: Bearer $CONDUIT_TOKEN"'
    logger.debug("Calling getTables, equivalent curl: {}".format(curlstring))
    data = getOnTheWire("/metadata/databases/{}/tables".format(database))
    if data != None:
        tables = []
        for table in data['tables']:
            tableObj = Table(table['table'], table['database'], table['schema'], table['tableType'])
            tables.append(tableObj)
        return tables
    else:
        logger.error("Error in the getDatabases call: curl would be {}".format(curlstring))

def getTableSchema(database, table):
    curlstring = 'curl -X GET "https://$CONDUIT_SERVER/query/metadata/databases/{database}/tables/{table}/schema" -H  "accept: application/json" -H "Authorization: Bearer $CONDUIT_TOKEN"'
    logger.debug("Calling getTableSchema, equivalent curl: {}".format(curlstring))
    data = getOnTheWire("/metadata/databases/{}/tables/{}/schema".format(database, table))
    if data != None:
        columns = []
        for column in data['columns']:
            obj = Column(column['name'], column['colType'], column['lengthOpt'], column['scaleOpt'], column['sqlType'])
            columns.append(obj)
        return columns
    else:
        logger.error("Error in the getDatabases call: curl would be {}".format(curlstring))

def token():
    if "CONDUIT_TOKEN" not in os.environ.keys():
         logger.error("CONDUIT_TOKEN is not set in environment.")
    else:
        return os.environ["CONDUIT_TOKEN"]

def server():
    if "CONDUIT_SERVER" not in os.environ.keys():
        logger.error("CONDUIT_SERVER is not set in environment.")
    else:
        return os.environ["CONDUIT_SERVER"]

def getDatabases():
    curlstring = 'curl -X GET "https://$CONDUIT_SERVER/query/metadata/databases" -H  "accept: application/json" -H "Authorization: Bearer $CONDUIT_TOKEN"'
    logger.debug("Calling getDatabases, equivalent curl: {}".format(curlstring))
    data = getOnTheWire("/metadata/databases")
    print(data)
    if data != None:
        dbs = []
        for db in data['databases']:
            dbObj = Database(db)
            dbs.append(dbObj)
        return dbs
    else:
        logger.error("Error in the getDatabases call: curl would be {}".format(curlstring))

if __name__ == "__main__":
    if token() == None or server() == None:
        raise Exception("You'll need to set the CONDUIT_TOKEN and CONDUIT_SERVER envvars to use this.")
    #obj = executeSyncQuery("SHOW DATABASES")
    #print(obj)

    #query = executeAsyncQuery("SELECT * FROM sql_synapse_flights.TransStats___Flights_All LIMIT 10000000")
    #cancelQuery(query)
    #query = executeAsyncQuery("SELECT * FROM sql_synapse_flights.TransStats___vw_airport_parsed LIMIT 1000000")
    #print(query)
    #if query.status == "Running":
    #    cancelQuery(query)

    dbs = getDatabases()
    for db in dbs:
         print(db)