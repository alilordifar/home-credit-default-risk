import os
import pandas as pd
import psycopg2
from psycopg2 import sql

class Postgres:
    def __init__(self, dbname, user, password, host='localhost', port=5432):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = psycopg2.connect(
            dbname='postgres',
            user=user,
            password=password,
            host=host,
            port=port
        )

    def create_db(self, new_dbname):
        try:
            connection = psycopg2.connect(
                dbname='postgres',
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(new_dbname)))
            cursor.close()
            connection.close()
            print(f"Database {new_dbname} created successfully")
        except Exception as error:
            print(f"Error: Could not create database. {error}")

    def connect_to_postgres(self):
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            print(f"Connected to PostgreSQL DB {self.dbname}")
            return self.connection
        except Exception as error:
            print(f"Error: Could not connect to PostgreSQL DB. {error}")
            return None

    def list_dbs(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT datname FROM pg_database;")
            databases = cursor.fetchall()
            cursor.close()
            return databases
        except Exception as error:
            print(f"Error: Could not list databases. {error}")
            return None
    def verify_row_count(self, schema_name, table_name):
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}.{}").format(sql.Identifier(schema_name), sql.Identifier(table_name)))
            row_count = cursor.fetchone()[0]
            cursor.close()
            print(f"Row count in table {table_name} under schema {schema_name}: {row_count}")
        except Exception as error:
            print(f"Error: Could not verify row count. {error}")

    def create_schema_and_table_from_csv(self, new_dbname, schema_name, csv_file_path):
        try:
            print("Creating database...")
            self.create_db(new_dbname)
            self.dbname = new_dbname
            print("Connecting to new database...")
            self.connect_to_postgres()
            cursor = self.connection.cursor()

            print("Creating schema...")
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
            self.connection.commit()

            print(f"Reading CSV file {csv_file_path.split('/')[-1]}...")
            df = pd.read_csv(csv_file_path, encoding='utf-8', encoding_errors='replace')
            columns = df.columns.tolist()
            dtypes = df.dtypes

            # Map pandas dtypes to PostgreSQL types
            dtype_mapping = {
                'int64': 'INTEGER',
                'float64': 'FLOAT',
                'object': 'TEXT',
                'bool': 'BOOLEAN',
                'datetime64[ns]': 'TIMESTAMP'
            }

            print("Creating table...")
            table_name = os.path.splitext(os.path.basename(csv_file_path))[0]
            create_table_query = sql.SQL(
                "CREATE TABLE IF NOT EXISTS {}.{} ({})"
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
                sql.SQL(', ').join(
                    sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(dtype_mapping[str(dtype)])) for col, dtype in zip(columns, dtypes)
                )
            )
            cursor.execute(create_table_query)
            self.connection.commit()
            print(f"Table {table_name} created successfully under schema {schema_name}")

            print("Inserting data...")
            for index, row in df.iterrows():
                insert_query = sql.SQL(
                    "INSERT INTO {}.{} ({}) VALUES ({})"
                ).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(map(sql.Identifier, columns)),
                    sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )
                cursor.execute(insert_query, row.tolist())
                if index % 200000 == 0:
                    print(f"Inserted {index} rows")
            self.connection.commit()

            cursor.close()
            print(f"Data inserted into table {table_name} under schema {schema_name} in database {new_dbname} successfully")

            # Verify row count
            self.verify_row_count(schema_name, table_name)
        except Exception as error:
            print(f"Error: Could not create schema or insert data. {error}")

# Example usage
if __name__ == "__main__":
    postgres = Postgres('postgres', 'postgres', 'password')
    postgres.create_schema_and_table_from_csv('home_credit_risk_db', 'home_credit_risk_raw', '/data/bureau_balance.csv')