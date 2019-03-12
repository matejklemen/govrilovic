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

    def testAddSite(self):
        domain_fake = 'https://ezcrawl_itsnotdotgov_soitsnotharmfulfor_prod_db.ru'
        robots_fake = 'User-agent: * Disallow: / search'
        sitemap_fake = '<urlset></urlset>'
        self.db.add_site_info_to_db(domain_fake,
                                    robots_fake,
                                    sitemap_fake)
        select_query = "SELECT domain, robots_content, sitemap_content FROM site WHERE domain = '" + \
            domain_fake + "';"

        self.assertEqual(str(self.db.return_one(select_query)),
                         "('"+domain_fake+"', '"+robots_fake+"', '"+sitemap_fake+"')")

        delete_test = "DELETE FROM site WHERE domain = '" + domain_fake + "';"

        self.db.alter(delete_test)
        self.db.close_connection()
