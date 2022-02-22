import os
import json
import csv
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
 
def main():
    
    data = load_json()
    csv_writer = load_csv()
    write_csv(data, csv_writer)
    
    values = get_data_from_csv()
    load_dotenv()
    run_sql(values)


def get_data_from_csv():
    csv_file = open('business_data.csv')
    csv_reader = csv.reader(csv_file)
    
    values = []
    
    for row in csv_reader:
        values.append((row[0], row[1]))
        
    return values
    
def load_json():
    
    with open('business100ValidForm.json') as file:
        json_data = json.load(file)
        
    file.close()
    
    business_data = json_data['Business']
    
    return business_data
    
    
def load_csv():
    
    csv_file = open('business_data.csv', 'w', newline='')
    csv_writer = csv.writer(csv_file)
    return csv_writer

def write_csv(data, csv_writer):
    
    header_row = True
    
    for json in data:
        if header_row:
            header = json.keys()
            csv_writer.writerow(header)
            header_row = False
    
        csv_writer.writerow(json.values())
        
def run_sql(values):
    
    create_table = """
            CREATE TABLE business (
                business_id VARCHAR(40) PRIMARY KEY,
                address VARCHAR(80) NOT NULL
            );
        """
        
    connection = create_server_connection(os.getenv('HOST'), os.getenv(
        'USER'), os.getenv('PASSWORD'), os.getenv('DATABASE'), os.getenv('PORT'))
    
    execute_query(connection, """DROP TABLE IF EXISTS business""")
    execute_query(connection, create_table)
    
    for value in values:
        sql = '''
            INSERT INTO business (business_id, address)
            VALUES (%s, %s)
            '''
            
        val = [(value[0], value[1])]
        execute_list_query(connection, sql, val)
    
def create_server_connection(host_name, user_name, user_password, db_name, port_num):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name,
            port=port_num
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


def execute_list_query(connection, sql, val):
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, val)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")
    
if __name__ == "__main__":
    
    main()




