import unittest
from Query_Processor.classes.query_processor import QueryProcessor
from Storage_Manager.classes.rows import Rows

class TestQueryProcessor(unittest.TestCase):

    def setUp(self):
        # Persiapan data dummy yang digunakan untuk setiap tes
        self.queryprocessor = QueryProcessor()
        self.dummy_data = [{'studentid': 1, 'studentname': 'John Doe', 'studentage': 20},
                           {'studentid': 2, 'studentname': 'Jane Doe', 'studentage': 21},
                           {'studentid': 3, 'studentname': 'John Smith', 'studentage': 22},
                           {'studentid': 4, 'studentname': 'Jane Smith', 'studentage': 23},
                           {'studentid': 5, 'studentname': 'John Doe', 'studentage': 24}]

        self.dummy_data2 = [{'studentid': 6, 'studentname': 'John Doe', 'studentage': 20},
                            {'studentid': 7, 'studentname': 'Jane Doe', 'studentage': 21},
                            {'studentid': 8, 'studentname': 'John Smith', 'studentage': 22}]

    def test_select(self):
        rows = Rows(self.dummy_data, len(self.dummy_data))

        result = self.queryprocessor.SELECT(rows, ['studentid'])

        self.assertEqual(result.data, [{'studentid': 1}, {'studentid': 2}, {'studentid': 3}, {'studentid': 4}, {'studentid': 5}])

    def test_limit(self):
        rows = Rows(self.dummy_data, len(self.dummy_data))

        result = self.queryprocessor.LIMIT(rows, 2)

        self.assertEqual(result.data, [{'studentid': 1, 'studentname': 'John Doe', 'studentage': 20},
                                      {'studentid': 2, 'studentname': 'Jane Doe', 'studentage': 21}])

    def test_order_by(self):
        rows = Rows(self.dummy_data, len(self.dummy_data))

        result = self.queryprocessor.ORDER_BY(rows, 'studentid', 'ASC')

        self.assertEqual(result.data, [{'studentid': 1, 'studentname': 'John Doe', 'studentage': 20},
                                      {'studentid': 2, 'studentname': 'Jane Doe', 'studentage': 21},
                                      {'studentid': 3, 'studentname': 'John Smith', 'studentage': 22},
                                      {'studentid': 4, 'studentname': 'Jane Smith', 'studentage': 23},
                                      {'studentid': 5, 'studentname': 'John Doe', 'studentage': 24}])

    def test_join(self):
        # Setup test data
        left_data = [
            {'studentid': 1, 'studentname': 'John Doe', 'studentage': 20},
            {'studentid': 2, 'studentname': 'Jane Doe', 'studentage': 21}
        ]
        right_data = [
            {'studentid': 1, 'coursename': 'Math', 'grade': 'A'},
            {'studentid': 2, 'coursename': 'Physics', 'grade': 'B'}
        ]
        
        left_rows = Rows(left_data, len(left_data))
        right_rows = Rows(right_data, len(right_data))

        # Execute
        result = self.queryprocessor.JOIN(left_rows, right_rows, ['studentid', 'studentid'])

        # Assert
        expected_result = [
            {
                'studentid': 1, 'studentname': 'John Doe', 'studentage': 20,
                'coursename': 'Math', 'grade': 'A'
            },
            {
                'studentid': 2, 'studentname': 'Jane Doe', 'studentage': 21,
                'coursename': 'Physics', 'grade': 'B'
            }
        ]
        
        self.assertEqual(result.data, expected_result)
        self.assertEqual(result.rows_count, len(expected_result))

if __name__ == '__main__':
    unittest.main()
