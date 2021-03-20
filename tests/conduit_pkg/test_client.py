import unittest
from unittest.mock import patch
from src.conduit_pkg.client import *
import json



def mockGetDatabases():
    pass

class TestClient(unittest.TestCase):
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




    # def test_executeQuery(self):
    #     responseFinished = '''{"queryId":"7bba5aec-2641-420e-be82-87015dcb0d7d","status":"Finished","message":null,"data":{"columns":["PassengerId","Survived","Pclass","Name","Sex","Age","SibSp","Parch","Ticket","Fare","Cabin","Embarked"],"rows":[{"PassengerId":1,"Name":"Braund, Mr. Owen Harris","Ticket":"A/5 21171","Pclass":3,"Parch":0,"Embarked":"S","Age":22,"Cabin":"","Fare":7.25,"SibSp":1,"Survived":0,"Sex":"male"},{"PassengerId":2,"Name":"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","Ticket":"PC 17599","Pclass":1,"Parch":0,"Embarked":"C","Age":38,"Cabin":"C85","Fare":71.2833,"SibSp":1,"Survived":1,"Sex":"female"},{"PassengerId":3,"Name":"Heikkinen, Miss. Laina","Ticket":"STON/O2. 3101282","Pclass":3,"Parch":0,"Embarked":"S","Age":26,"Cabin":"","Fare":7.925,"SibSp":0,"Survived":1,"Sex":"female"},{"PassengerId":4,"Name":"Futrelle, Mrs. Jacques Heath (Lily May Peel)","Ticket":"113803","Pclass":1,"Parch":0,"Embarked":"S","Age":35,"Cabin":"C123","Fare":53.1,"SibSp":1,"Survived":1,"Sex":"female"},{"PassengerId":5,"Name":"Allen, Mr. William Henry","Ticket":"373450","Pclass":3,"Parch":0,"Embarked":"S","Age":35,"Cabin":"","Fare":8.05,"SibSp":0,"Survived":0,"Sex":"male"},{"PassengerId":6,"Name":"Moran, Mr. James","Ticket":"330877","Pclass":3,"Parch":0,"Embarked":"Q","Age":60,"Cabin":"","Fare":8.4583,"SibSp":0,"Survived":0,"Sex":"male"},{"PassengerId":7,"Name":"McCarthy, Mr. Timothy J","Ticket":"17463","Pclass":1,"Parch":0,"Embarked":"S","Age":54,"Cabin":"E46","Fare":51.8625,"SibSp":0,"Survived":0,"Sex":"male"},{"PassengerId":8,"Name":"Palsson, Master. Gosta Leonard","Ticket":"349909","Pclass":3,"Parch":1,"Embarked":"S","Age":2,"Cabin":"","Fare":21.075,"SibSp":3,"Survived":0,"Sex":"male"},{"PassengerId":9,"Name":"Johnson, Mrs. Oscar W (Elisabeth Vilhelmina Berg)","Ticket":"347742","Pclass":3,"Parch":2,"Embarked":"S","Age":27,"Cabin":"","Fare":11.1333,"SibSp":0,"Survived":1,"Sex":"female"},{"PassengerId":10,"Name":"Nasser, Mrs. Nicholas (Adele Achem)","Ticket":"237736","Pclass":2,"Parch":0,"Embarked":"C","Age":14,"Cabin":"","Fare":30.0708,"SibSp":1,"Survived":1,"Sex":"female"}],"hasNext":true,"hasPrevious":false}}'''
    #     with patch('requests.get') as mock_request:
    #         mock_request.return_value.status_code = 200
    #         mock_request.return_value.json.return_value = json.loads(responseFinished)
    #         sqlString = "TEST SQL DUMMY STRING"
    #         obj = executeQuery(sqlString, 10, 10)
    #         self.assertTrue(obj.status, "Finished")
    #         self.assertEqual(2, len(obj.data['rows']))
    #         self.assertEqual("es_conduit", obj.data['rows'][1]['Result'])


if __name__ == '__main__':
    unittest.main()