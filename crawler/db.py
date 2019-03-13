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
            self.connection = psycopg2.connect(user=self.user,
                                               password=self.password,
                                               host=self.host,
                                               port=self.port,
                                               database=self.db)
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
        self.connection.close()
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
        except:
            print("Failed to run parameterized query: ", query)
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

    # Helpers for adding pages to the database
    def add_site_info_to_db(self, domain, robots, sitemap):
        insert_parameterized_query = "INSERT INTO site (domain, robots_content, sitemap_content) VALUES (%s, %s, %s)"
        self.param_query(insert_parameterized_query, [domain, robots, sitemap])



if __name__ == "__main__":
    db = Database()
    db.close_connection()
