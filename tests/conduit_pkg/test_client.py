import unittest
from unittest.mock import patch
from src.conduit_pkg.client import *
import json



def mockGetDatabases():
    pass
class TestCancel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logger.setLevel(level=logging.DEBUG)
        cls.env_patcher = patch.dict(os.environ, {"CONDUIT_TOKEN": "blah", "CONDUIT_SERVER": "serverthing"})
        cls.env_patcher.start()
    @classmethod
    def tearDownClass(self):
        self.env_patcher.stop()

    def test_token(self):
        expected = "blah"
        actual = token()
        self.assertEqual(expected, actual)

    def test_server(self):
        expected = "serverthing"
        actual = server()
        self.assertEqual(expected, actual)

    def test_getDatabases(self):
        response = '''{
  "databases": [
    "file_blob",
    "redshift_redshift"
  ]
}'''
        with patch('requests.get') as mock_request:
            mock_request.return_value.status_code = 200
            mock_request.return_value.json.return_value = json.loads(response)
            objs = getDatabases()
            self.assertEqual(2, len(objs))

    def test_getTableSchema(self):
        response = '''{
  "columns": [
    {
      "name": "_c109",
      "colType": "nvarchar",
      "lengthOpt": null,
      "scaleOpt": null,
      "sqlType": 1111
    },
    {
      "name": "ActualElapsedTime",
      "colType": "float",
      "lengthOpt": null,
      "scaleOpt": null,
      "sqlType": 8
    }]
}
'''
        with patch('requests.get') as mock_request:
            mock_request.return_value.status_code = 200
            mock_request.return_value.json.return_value = json.loads(response)
            objs = getTableSchema('test', 'test')
            self.assertEqual(2, len(objs))


    def test_getTables(self):
        response = '''{
      "tables": [
        {
          "table": "TransStats___vw_airport_parsed",
          "database": "sql_synapse_flights",
          "schema": "sql_synapse_flights",
          "tableType": "TABLE"
        },
        {
          "table": "TransStats___dimCarriers",
          "database": "sql_synapse_flights",
          "schema": "sql_synapse_flights",
          "tableType": "TABLE"
        }
      ]
    }
    '''
        with patch('requests.get') as mock_request:
            mock_request.return_value.status_code = 200
            mock_request.return_value.json.return_value = json.loads(response)
            objs = getTables('test')
            self.assertEqual(2, len(objs))

    def test_executeSyncQuery(self):
        response = '''{
  "queryId": "ac99e49f-09bd-47d4-b427-0c76f5adc1a4",
  "status": "Finished",
  "message": null,
  "data": {
    "columns": [
      "Result"
    ],
    "rows": [
      {
        "Result": "awss3_s3"
      },
      {
        "Result": "es_conduit"
      }
    ],
    "hasNext": false,
    "hasPrevious": false
  }
}
'''
        with patch('requests.get') as mock_request:
            mock_request.return_value.status_code = 200
            mock_request.return_value.json.return_value = json.loads(response)
            sqlString = "TEST SQL DUMMY STRING"
            obj = executeSyncQuery(sqlString)
            self.assertTrue(obj.status, "Finished")
            self.assertEqual(2, len(obj.data['rows']))
            self.assertEqual("es_conduit", obj.data['rows'][1]['Result'])

    def test_executeAsyncQuery(self):
        finishedResponse = '''{
  "queryId": "ac99e49f-09bd-47d4-b427-0c76f5adc1a4",
  "status": "Finished",
  "message": null,
  "data": {
    "columns": [
      "Result"
    ],
    "rows": [
      {
        "Result": "awss3_s3"
      },
      {
        "Result": "es_conduit"
      }
    ],
    "hasNext": false,
    "hasPrevious": false
  }
}
'''
        with patch('requests.get') as mock_request:
            mock_request.return_value.status_code = 200
            mock_request.return_value.json.return_value = json.loads(finishedResponse)
            sqlString = "TEST SQL DUMMY STRING"
            obj = executeAsyncQuery(sqlString)
            self.assertTrue(obj.queryId, "ac99e49f-09bd-47d4-b427-0c76f5adc1a4")

    def test_cancelQuery(self):
        queryResponse = '''{
          "queryId": "ac99e49f-09bd-47d4-b427-0c76f5adc1a4",
          "status": "Running",
          "message": null,
          "data": null
}
'''
        response = '''{"isCancelled": true}'''
        with patch('requests.get') as mock_request:
            mock_request.return_value.status_code = 200
            mock_request.return_value.json.return_value = json.loads(response)
            qObj = QueryResult(json.loads(queryResponse))
            cancelled = cancelQuery(qObj)
            self.assertTrue(cancelled)

if __name__ == '__main__':
    unittest.main()