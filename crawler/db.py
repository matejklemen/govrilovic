import psycopg2

class Database:

    host = "localhost"
    user = 'postgres'
    password = 'postgres'
    port = "5432"
    db = 'crawldb'

    def __init__(self):
        try:
            self.connection = psycopg2.connect(user = self.user,
                                        password = self.password,
                                        host = self.host,
                                        port = self.port,
                                        database = self.db)
            self.cursor = self.connection.cursor()
            print("Connected to database ", self.db)
        except (Exception, psycopg2.Error) as error :
            print("Error while connecting to PostgreSQL", error)
        finally:
            #closing database connection.
            if self.connection:
                self.cursor.close()
                self.connection.close()
                print("PostgreSQL connection closed")
        
    # Method for create/update/delete (CUD) queries
    def insert(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except:
            self.connection.rollback()


    # Method for return (R) query (Single result). 
    def return_one(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchone()

    # Method for return (R) query (All results). 
    def return_all(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def truncate_everything(self):
        # TODO
        pass




if __name__ == "__main__":
    db = Database()