import unittest
from crawler.db import Database


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = Database()

    def testInsertDelete(self):
        insert_test = "INSERT INTO data_type (code) VALUES ('TEST_VALUE');"
        select_all_query = "SELECT * FROM data_type;"
        delete_test = "DELETE FROM data_type WHERE code = 'TEST_VALUE';"

        # Insert statement test
        self.db.alter(insert_test)        
        self.assertEqual(str(self.db.return_all(select_all_query)),
         "[('PDF',), ('DOC',), ('DOCX',), ('PPT',), ('PPTX',), ('TEST_VALUE',)]")
        # Delete statement test
        self.db.alter(delete_test)
        self.assertEqual(str(self.db.return_all(select_all_query)),
         "[('PDF',), ('DOC',), ('DOCX',), ('PPT',), ('PPTX',)]")
        #  Close the connection
        self.db.close_connection()
