import os, logging, requests, urllib.parse, time, json

logging.basicConfig(
    format="%(name)s-%(levelname)s-%(asctime)s-%(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class QueryResult(object):
    def __init__(self, jsonLoad):
        self.json = jsonLoad
        self.parseDict()
    def parseDict(self):
        self.queryId = self.json['queryId']
        self.status = self.json['status']
        self.message = self.json['message']
        self.data = self.json['data']
        if self.data != None and type(self.data) == dict and len(self.data) > 0:
            self.hasNext = self.data['hasNext']
            self.hasPrevious = self.data['hasPrevious']
    def __str__(self):
        return "QueryId: {}, with status: {}, has message: {}, check 'data' attribute for data.".format(
            self.queryId, self.status, self.message
        )

class Query(object):
    def __init__(self, server, token, sqlstring, pagesize=100, timeout=30):
        self.Server = server
        self.Token = token
        self.SQLString = sqlstring
        self.PageSize = pagesize
        self.Timeout = timeout
        self.QueryId = None
        self.QueryResult = None
        self.StartTime = None
        self.DataSlices = []
        self.MaxPageSize = 1000
        self.PageSize = pagesize if pagesize < self.MaxPageSize else self.MaxPageSize
    def __str__(self):
        return "Query has ID {}, Pagesize {}".format(self.QueryId, self.PageSize)

    def cancelQuery(self):
        logger.info("Forcing a cancel due to exceeding timeout...")
        self.DataSlices = []
        headers = {
            'accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.Token)
        }

        url = 'https://{}/api/query/cancel?queryId={}'.format(self.Server, self.QueryId)
        resp = requests.get(url, headers=headers)
        jsondict = json.loads(resp.text)
        if type(jsondict) == dict:
            if jsondict['isCancelled'] == False:
                logger.info("Query {} isn't cancelled yet...checking again".format(self.QueryId))
                time.sleep(2)
                self.cancelQuery()
            elif jsondict['isCancelled'] == True:
                logger.info("Query {} successfully cancelled!!".format(self.QueryId))
                return True
            else:
                logger.info("Query in a strange state: {}".format(jsondict['isCancelled']))
                return True
    def timedOut(self):
        if self.StartTime == None:
            self.StartTime = time.time()
            return False
        else:
            elapsed_time = time.time() - self.StartTime
            if elapsed_time >= self.Timeout:
                return True
            else:
                return False

    def checkQuery(self):
        if self.timedOut():
            self.cancelQuery()
            return

        headers = {
            'accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.Token)
        }
        url = 'https://{}/api/query/execute/{}/result'.format(self.Server, self.QueryId)
        logger.info("Getting URL: {}".format(url))
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            if type(data) == dict:
                self.processQueryResult(data)
        else:
            logger.error("There was an error posting query:{}".format(resp.status_code))


    def executeQuery(self):
        if self.timedOut():
            self.cancelQuery()
            return

        headers = {
            'accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.Token)
        }
        url = 'https://{}/api/query/execute'.format(self.Server)
        payload = dict()
        payload["queryId"] = self.QueryId
        payload["query"] = self.SQLString
        payload["pageSize"] = self.PageSize
        logger.info("Posting URL: {}, with payload: {}".format(url, payload))
        resp = requests.post(url, headers=headers, json=payload)

        if resp.status_code == 200:
            data = resp.json()
            if type(data) == dict:
                self.processQueryResult(data)
        else:
            logger.error("There was an error posting query:{}".format(resp.status_code))

    def processQueryResult(self, dataDict):
        self.QueryResult = QueryResult(dataDict)
        self.QueryId = self.QueryResult.queryId
        self.DataSlices.append(dataDict)
        logger.info("Processing query...{}".format(self.QueryResult.queryId))

        if self.QueryResult.status == "Finished" or self.QueryResult.status == "ResultsReady":
            if self.QueryResult.hasNext:
                logger.info("Query is finished, but has more, so paging...")
                print(self)
                self.executeQuery()
        elif self.QueryResult.status == "Running":
            time.sleep(2)
            logger.info("Query is Running, need to poll for completion...")
            self.checkQuery()
        else:
            logger.info("Query isn't running or finished: {}".format(self))


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


def getAllMetadata():
    raise NotImplementedError("TODO: getAllMetadata not implemented yet.")

def getOnTheWire(endpoint):
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer {}'.format(token())
    }
    url = 'https://{}/api{}'.format(server(), endpoint)
    resp = requests.get(url, headers = headers)
    if resp.status_code == 200:
        data = resp.json()
        if data is None:
            data = ""
        return data
    else:
        logger.error("There was an error calling endpoint {}".format(endpoint))

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
    #print(data)
    if data != None:
        dbs = []
        for db in data['databases']:
            dbObj = Database(db)
            dbs.append(dbObj)
        return dbs
    else:
        logger.error("Error in the getDatabases call: curl would be {}".format(curlstring))

def executeQuery(sqlstring, pageSize, timeout):
    """Executes a query on the query/execute Conduit endpoint.

    :param sqlstring: The SQL String you wish to execute. You may want to put a LIMIT clause on it.
    :param pageSize: This is the pagination window that you desire.
    :param timeout: This is the value (in seconds) that you wish to wait before canceling.
    :return: This returns a list of dictionaries of the appropriate window size.
    """
    query = Query(server(), token(), sqlstring, pageSize, timeout)
    query.executeQuery()
    data = query.DataSlices
    return data

if __name__ == "__main__":
    if token() == None or server() == None:
        raise Exception("You'll need to set the CONDUIT_TOKEN and CONDUIT_SERVER envvars to use this.")
    #obj = executeQuery("SHOW DATABASES", 10, 10)
    #print(obj)
    #query = executeSyncQuery("SELECT * FROM sql_synapse_flights.TransStats___Flights_All LIMIT 1000")
    # tables = getTables("file_costi_blob")
    # for tbl in tables:
    #     print(tbl)

    #data = executeQuery("SELECT * FROM `file_costi_blob`.`titanic.csv` LIMIT 50000", 100000, 1)
    # data = executeQuery("SELECT * FROM `oracle_flights`.`PDBADMIN___FLIGHTS` LIMIT 5000", 10000, 100)
    # for slice in data:
    #      print(slice)

    #cancelQuery(query)
    #query = executeAsyncQuery("SELECT * FROM sql_synapse_flights.TransStats___vw_airport_parsed LIMIT 1000000")

    #print(query)
    #print(query.data)
    #if query.status == "Running":
    #    cancelQuery(query)
    #
    # dbs = getDatabases()
    # for db in dbs:
    #      print(db)
    #
    # tables = getTables('sql_x_challenge')
    # for table in tables:
    #     print(table)
    #
    # cols = getTableSchema('sql_x_challenge', 'dbo___vTargetMail')
    # for c in cols:
    #     print(c)