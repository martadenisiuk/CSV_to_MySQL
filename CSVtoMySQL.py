import json
import pandas as pd
import os
from datetime import timedelta
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

path = os.path.dirname(os.path.realpath('__file__'))

# Load the JSON file
with open(path + "\\config.json", "r") as file:
    config = json.load(file)

# Retrieve database configuration
host_name = config["host_name"]
user_name = config["user_name"]
user_password = config["user_password"]
database = config["database"]
list_of_tables = config["list_of_tables"]

print(f"Connecting to database {database} on server {host_name} as {user_name}")

def create_server_engine(host_name, user_name, user_password, database):
    
    engine = None
    try:
        path_sql = "mysql+mysqlconnector://{}:{}@{}/{}".format(user_name, user_password, host_name, database)
        engine = create_engine(path_sql)
        print("MySQL Database connection successful")
    except SQLAlchemyError as err:
        error = str(err.__dict__['orig'])
        print(f"Error: '{error}'")

    return engine


def create_table_sql(df, conn, file_name, if_exists='replace'):

    # Retrieve the starting date from the 'analiza' table
    get_start_date = text("SELECT start_date_time from analiza where id = 1;")
    start_date = conn.execute(get_start_date).fetchall()[0][0]

    # Add a column datetime
    hours = df['h'].apply(lambda x: int(x[1:]) - 1)
    df['hour_date_time'] = [start_date + timedelta(hours=i) for i in hours]

    # Insert the DataFrame into the SQL database and create constraints
    df.to_sql(file_name.lower(), conn, if_exists=if_exists, index=False)

    add_index = text(f"""ALTER TABLE {file_name.lower()} ADD COLUMN ID INT(11) UNSIGNED 
                     AUTO_INCREMENT FIRST,
                      ADD CONSTRAINT pk_{file_name.lower()} PRIMARY KEY (ID);""")
    conn.execute(add_index)

    # Create FOREIGN KEY
    create_fk = text(f"""ALTER TABLE {file_name.lower()} 
                     ADD COLUMN id_analizy INT(11) UNSIGNED, 
                     ADD CONSTRAINT FK_Analiza{file_name.lower()} FOREIGN KEY (id_analizy)
                      REFERENCES analiza(id);""")
    conn.execute(create_fk)

    fill_fk = text(f"UPDATE {file_name.lower()} SET id_analizy = 1;")

    conn.execute(fill_fk)
    conn.commit()

    print(f"Created table {file_name.lower()}\n")



def replace_inf_with_null(df):

    # The database does not support 'inf' or '-inf' values.
    # An error was encountered due to these values being present in the dataset.
    # To prevent this issue, the function will replace 'inf' and '-inf' with NULL before inserting into SQL.
    df1 = df.copy()
    df1 = df1.map(lambda x: np.NAN if x in [np.inf, (-np.inf)] else x)

    return df1


def add_records(df, conn, file_name):

    # Retrieve the starting date from the 'analiza' table
    get_start_date = text("SELECT start_date_time from analiza where id = 1;")
    start_date = conn.execute(get_start_date).fetchall()[0][0]

    # Add a column datetime
    hours = df['h'].apply(lambda x: int(x[1:]) - 1)
    df['hour_date_time'] = [start_date + timedelta(hours=i) for i in hours]
    df['id_analizy'] = 1

    # Insert records into the SQL database
    df.to_sql(file_name.lower(), conn, if_exists='append', index=False)
    conn.commit()

    print(f"Updated table {file_name.lower()}\n")


if __name__ == '__main__':

    engine = create_server_engine(user_name=user_name,
                                  user_password=user_password,
                                  host_name=host_name,
                                  database=database)

    for file_name in list_of_tables:

        df = pd.read_csv(path + file_name + ".csv")

        while True:

            with engine.connect() as connection:

                try:

                    if engine.dialect.has_table(connection, file_name) \
                            and len(engine.dialect.get_indexes(connection, file_name)) != 0:

                        add_records(df=df, conn=connection, file_name=file_name)
                        connection.close()
                        break

                    else:

                        create_table_sql(df=df,
                                         conn=connection,
                                         file_name=file_name)
                        connection.close()
                        break

                except sqlalchemy.exc.ProgrammingError as err:

                    print("An error occurred! Fixing the issue")
                    df = replace_inf_with_null(df)
                    # print(f'\nError: {err}')

                    connection.close()

    engine.dispose()
