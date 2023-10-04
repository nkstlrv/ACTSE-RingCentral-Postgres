import psycopg2


class PostgresHandler:
    def __init__(self, db_params):
        self.db_params = db_params

    def __enter__(self):
        try:
            self.connection = psycopg2.connect(**self.db_params)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while connecting to PostgreSQL:", error)
            raise
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()

    def execute(self, sql_query, params=None):
        try:
            with self.connection.cursor() as cursor:
                if params:
                    cursor.execute(sql_query, params)
                else:
                    cursor.execute(sql_query)
                self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error executing SQL query:", error)


if __name__ == "__main__":
    db_config = {
        "host": "localhost",
        "database": "postgres",
        "user": "postgres",
        "password": "postgres",
        "port": 5432
    }

    with PostgresHandler(db_config) as db_handler:
        sql_query = """CREATE TABLE IF NOT EXISTS test(
            id serial PRIMARY KEY,
            number INT
        );"""
        db_handler.execute(sql_query)

        db_handler.execute(
            """INSERT INTO test (number) VALUES (
                50
            )"""
        )