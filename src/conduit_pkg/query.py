import asyncio, logging, requests, time

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
        self.hasNext = self.data['hasNext']
        self.hasPrevious = self.data['hasPrevious']
    def __str__(self):
        return "QueryId: {}, with status: {}, has message: {}, check 'data' attribute for data.".format(
            self.queryId, self.status, self.message
        )

class Query(object):
    def __init__(self, server, token, sqlstring, windowsize=100):
        self.Server = server
        self.Token = token
        self.SQLString = sqlstring
        self.WindowSize = windowsize
        self.Offset = 0
        self.QueryId = None
        self.QueryResult = None
        self.DataSlices = []
        self.MaxWindowSize = 10
        self.WindowSize = windowsize if windowsize < self.MaxWindowSize else self.MaxWindowSize
    def __str__(self):
        return "Query has ID {}, Offset {}, Windowsize {}".format(self.QueryId, self.Offset, self.WindowSize)

    def executeQuery(self):
        headers = {
            'accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.Token)
        }
        if self.QueryId == None or len(self.DataSlices) > 0:
            url = 'https://{}/query/execute'.format(self.Server)
            payload = dict()
            payload["queryId"] = self.QueryId
            payload["query"] = self.SQLString
            payload["offset"] = self.Offset
            payload["limit"] = self.WindowSize
            logger.info("Posting URL: {}, with payload: {}".format(url, payload))
            resp = requests.post(url, headers=headers, json=payload)
        else:
            url = 'https://{}/query/execute/{}/result'.format(self.Server, self.QueryId)
            logger.info("Getting URL: {}".format(url))
            resp = requests.get(url, headers=headers)

        if resp.status_code == 200:
            data = resp.json()
            if type(data) == dict:
                self.processQueryResult(data)
                # if self.QueryResult.status == "Finished" and not self.QueryResult.hasNext:
                #     self.finishQueryCall()
        else:
            logger.error("There was an error posting query:{}".format(resp.status_code))

    def processQueryResult(self, dataDict):

        self.QueryResult = QueryResult(dataDict)
        self.DataSlices.append(dataDict)
        if self.QueryResult.status == "Finished":
            logger.info("Finished Query...")
            if self.QueryResult.hasNext:
                logger.info("Query is finished, but has more, so paging...")
                self.Offset = self.Offset + self.WindowSize
                self.QueryId = self.QueryResult.queryId
                print(self)
                self.executeQuery()
        elif self.QueryResult.status == "Running":
            time.sleep(2)
            logger.info("Query is Running, need to poll for completion...")
            self.QueryId = self.QueryResult.queryId
            self.executeQuery()
        else:
            logger.info("Query isn't running or finished: {}".format(self))

