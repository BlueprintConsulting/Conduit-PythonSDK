import unittest
from src.conduit_pkg.conduit import HelloWorld

class TestHelloWorld(unittest.TestCase):
    def test_helloworld(self):
        expected = "blah"
        actual = HelloWorld(expected)
        self.assertEqual(expected, actual)

if __name__ == '__main__':
    unittest.main()