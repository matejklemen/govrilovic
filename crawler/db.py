import psycopg2

class Database:

    host = "localhost"
    user = 'postgres'
    password = 'postgres'
    port = "5432"
    db = 'crawldb'
    schema = 'crawldb'

    def __init__(self):
        try:
            self.connection = psycopg2.connect(user = self.user,
                                        password = self.password,
                                        host = self.host,
                                        port = self.port,
                                        database = self.db)
            self.cursor = self.connection.cursor()
            # Set the schema to 'crawldb' so we don't have to specify in each query.
            set_schema = "SET search_path TO " + self.schema
            self.alter(set_schema)
            print("Connected to database ", self.db)
        except (Exception, psycopg2.Error) as error :
            print("Error while connecting to PostgreSQL", error)

    # Close database connection
    def close_connection(self):
        self.cursor.close()
        self.connection.close()
        print("PostgreSQL connection closed")

    # Method for create/update/delete (CUD) queries
    def alter(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except:
            print("Failed to run query: ", query)
            self.connection.rollback()


    # Method for return (R) query (Single result). 
    def return_one(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchone()

    # Method for return (R) query (All results). 
    def return_all(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Clear the information in database.
    # Tables NOT to clear: data_type, page_type
    # Tables to truncate: link, image, page_data, page, site
    def truncate_everything(self):
        # TODO
        pass

    # Basic insert, delete and select all tests.
    def test_db(self):
        insert_test = "INSERT INTO data_type (code) VALUES ('TEST_VALUE');"
        select_all_query = "SELECT * FROM crawldb.data_type;"
        delete_test = "DELETE FROM data_type WHERE code = 'TEST_VALUE';"

        self.alter(insert_test)        
        if str(self.return_all(select_all_query)) == "[('PDF',), ('DOC',), ('DOCX',), ('PPT',), ('PPTX',), ('TEST_VALUE',)]":
            print("Insert OK")
            self.alter(delete_test)
            if str(self.return_all(select_all_query)) == "[('PDF',), ('DOC',), ('DOCX',), ('PPT',), ('PPTX',)]":
                print("Delete OK")
        else:
            print("Insert FAIL")

if __name__ == "__main__":
    db = Database()
    db.test_db()
    db.close_connection()