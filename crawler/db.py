import psycopg2
from psycopg2 import pool
from datetime import datetime
import difflib

class Pool:

    host = "localhost"
    user = 'postgres'
    password = 'postgres'
    port = "5432"
    db = 'crawldb'
    schema = 'crawldb'

    def __init__(self):
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(1, 100,
                                                             user=self.user,
                                                             password=self.password,
                                                             host=self.host,
                                                             port=self.port,
                                                             database=self.db)
            print("Connected to database ", self.db, " and created pool.")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)


class Database:

    db = 'crawldb'
    schema = 'crawldb'

    def __init__(self, pool):
        try:
            self.pool = pool
            self.connection = self.pool.pool.getconn()
            self.cursor = self.connection.cursor()
            # Set the schema to 'crawldb' so we don't have to specify in each query.
            set_schema = "SET search_path TO " + self.schema
            self.alter(set_schema)
            print("Connected to database ", self.db)
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    # Close database connection
    def close_connection(self):
        self.cursor.close()
        self.pool.putconn(self.connection)
        print("PostgreSQL connection closed")

    # Not safe - perform self query escapes etc.
    # Method for create/update/delete (CUD) queries
    def alter(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except:
            print("Failed to run query: ", query)
            self.connection.rollback()

    # Parameterized queries - sql injection safe. Use this from now on.
    def param_query(self, query, parameters):
        """ 
        Parameters
        ----------
        query: str
            Parameterized query statement. Example: "INSERT INTO x VALUES (%s, %s, ... , %s)

        parameters: list/array
            Values to be placed into statement. For example: [val1, val2, ... , valN]

        -------
        """
        try:
            self.cursor.execute(query, parameters)
            self.connection.commit()
        except Exception as e:
            print("Failed to run parameterized query: ", query)
            print("Issue ", e)
            self.connection.rollback()

    # Returns the current time.
    def current_time(self):
        return datetime.now()

    # Method for return (R) query (Single result).
    def return_one(self, query, parameters):
        try:
            self.cursor.execute(query, parameters)
            return self.cursor.fetchone()
        except Exception as e:
            print("Return one failed ", e)
            return None

    # Method for return (R) query (All results).
    def return_all(self, query, parameters):
        try:
            self.cursor.execute(query, parameters)
            return self.cursor.fetchone()
        except Exception as e:
            print("Return all failed ", e)
            return None

    # Clear the information in database.
    # Tables NOT to clear: data_type, page_type
    # Tables to truncate: link, image, page_data, page, site
    def truncate_everything(self):
        query = "TRUNCATE link, image, page_data, page, site"
        self.alter(query)
        print("Database Truncated.")

    def root_site_id(self, root_site):
        query = "SELECT id FROM site WHERE domain = (%s)"
        return self.return_one(query, [root_site])

    # Helper function for inserting links (duplicate page links to the OG page)
    def add_link(self, from_page_id, to_page_id):
        query = """INSERT INTO link (from_page, to_page) 
                   VALUES (%s, %s)
                """
        self.param_query(query, [from_page_id, to_page_id])

    # Helper function for inserting an image into the database.
    def add_image(self, page_url, filename, content_type, data):
        accessed_time = self.current_time()
        page_id = self.return_one(
            "SELECT id FROM page WHERE url = (%s)", [page_url])
        if page_id != None:
            insert_parameterized_query = """
                    INSERT INTO image (page_id, filename, content_type, data, accessed_time) 
                    VALUES (%s, %s, %s, %s, %s)
                    """
            self.param_query(insert_parameterized_query, [
                             page_id, filename, content_type, data, accessed_time])

    # Helper function for inserting page data

    def add_page_data(self, page_url, data_type_code, data):
        # Return foreign key ID of this page.
        page_id = self.return_one(
            "SELECT id FROM page WHERE url = (%s)", [page_url])
        if page_id != None:
            insert_parameterized_query = """INSERT INTO page (page_id, data_type_code, data) 
            VALUES (%s, %s, %s)"""
            self.param_query(insert_parameterized_query, [
                             page_id, data_type_code, data])

    # Helpers for adding pages to the database
    def add_site_info_to_db(self, domain, robots, sitemap):
        if self.root_site_id(domain) == None:
            insert_parameterized_query = "INSERT INTO site (domain, robots_content, sitemap_content) VALUES (%s, %s, %s)"
            self.param_query(insert_parameterized_query,
                             [domain, robots, sitemap])

    # Helper for adding a page into the database
    def add_page(self, site_id, page_type_code, url, html_content, http_status_code, lsh_hash):
        accessed_time = self.current_time()
        insert_parameterized_query = """INSERT INTO page (site_id, page_type_code, url, html_content, http_status_code, 
        accessed_time, lsh_hash) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        self.param_query(insert_parameterized_query, [
                         site_id, page_type_code, url, html_content, http_status_code, accessed_time, lsh_hash])

    def add_link_between_two_sites(self, page_og_url, page_dup_url):
        og_page_id = self.return_one(
            "SELECT id FROM page WHERE url = (%s)", [page_og_url])
        dup_page_id = self.return_one(
            "SELECT id FROM page WHERE url = (%s)", [page_dup_url])
        if og_page_id != None and dup_page_id != None:
            insert_parameterized_query = """INSERT INTO link (from_page, to_page) VALUES (%s, %s)"""
            self.param_query(insert_parameterized_query,
                             [og_page_id, dup_page_id])

    def compare_lsh(self, new_lsh):
        pass


    def same_lsh_sites(self, lsh, content1, url,status_code):
        '''
        Finds all the sites with same lsh_hash and compares their html_content to its own.
        It uses SequenceMatcher to calculate simillarity.
        Sites are equal if ratio is more or equal to 0.9.

        Also adds the page into the database and creates entry into Links table.
        '''
        same_lsh_website_ids = self.return_all("SELECT id FROM page WHERE lsh_hash = (%s)", [lsh])
        if same_lsh_website_ids != None:
            # Go through all returned sites
            for id in same_lsh_website_ids:
                html_content2 = self.return_all("SELECT html_content, url FROM page WHERE id= (%s)", [id])
                if html_content2 != None:
                    simillarity=difflib.SequenceMatcher(a=content1.lower(), b=html_content2[0].lower())
                    if simillarity.ratio() >= 0.9:
                        print("Duplicate page found.")
                        root_site_id = self.root_site_id(url)
                        self.add_page(root_site_id, "DUPLICATE", url, None, status_code, lsh)
                        # Insert into links
                        self.add_link_between_two_sites(html_content2[1], url)
                        print(html_content2[1])
                        return True
                    
                

        return False

if __name__ == "__main__":
    # db = Database()
    # db.close_connection()
    pass
