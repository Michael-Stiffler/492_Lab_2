import os
import json
import csv
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
 
def main():
    
    data = load_json()
    write_csv(data)
    
    load_dotenv()
    run_sql()
    
    
def load_json():
    
    with open('business100ValidForm.json') as file:
        json_data = json.load(file)
        
    file.close()
    
    business_data = json_data['Business']
    
    return business_data
    
    
def load_csv(filename):
    
    csv_file = open(filename, 'w', newline='')
    csv_writer = csv.writer(csv_file)
    return csv_writer

def write_csv(data):
    
    business_data_writer = load_csv('business_data.csv')
    hours_data_writer = load_csv('hours_data.csv')
    categories_data_writer = load_csv('categories_data.csv')
    attributes_data_writer = load_csv('attributes_data.csv')
     
    header_row = True
    
    for json in data:
        if header_row:
            header = json.keys()

            header = list(header)

            list_to_write = [header[0], header[1], header[3], header[5], header[6], header[7], header[8], header[9], header[10], header[11], header[12], header[14]]
            business_data_writer.writerow(list_to_write)  
            
            list_to_write = [header[0], header[2]]
            hours_data_writer.writerow(list_to_write)
        
            list_to_write = [header[0], header[4]]
            categories_data_writer.writerow(list_to_write)   
            
            list_to_write = [header[0], header[13]]
            attributes_data_writer.writerow(list_to_write)  
            
            header_row = False
        
        list_to_write = [json['business_id'], json['full_address'], json['open'], json['city'], json['review_count'], json['name'], json['neighborhoods'], json['longitude'], json['state'], json['stars'], json['latitude'], json['type']]
        business_data_writer.writerow(list_to_write) 
            
        list_to_write = [json['business_id'], json['hours']]  
        hours_data_writer.writerow(list_to_write)
            
        list_to_write = [json['business_id'], json['categories']]     
        categories_data_writer.writerow(list_to_write)   
            
        list_to_write = [json['business_id'], json['attributes']]     
        attributes_data_writer.writerow(list_to_write)  
        
        
def get_data_from_business_csv():
    csv_file = open('business_data.csv')
    csv_reader = csv.reader(csv_file)
    
    header = True
    values = []
    
    for row in csv_reader:
        if header:
            header = False
        else:
            values.append(row)
        
    return values


def get_data_from_hours_csv():
    csv_file = open('hours_data.csv')
    csv_reader = csv.reader(csv_file)
    
    values = []
    
    for row in csv_reader:
        values.append((row[0], row[1]))
        
    return values


def get_data_from_categories_csv():
    csv_file = open('categories_data.csv')
    csv_reader = csv.reader(csv_file)
    
    values = []
    
    for row in csv_reader:
        values.append((row[0], row[1]))
        
    return values


def get_data_from_attributes_csv():
    csv_file = open('attributes_data.csv')
    csv_reader = csv.reader(csv_file)
    
    values = []
    
    for row in csv_reader:
        values.append((row[0], row[1]))
        
    return values

        
def run_sql():
    
    business_data = get_data_from_business_csv()
    #hours_data = get_data_from_hours_csv()
    #categories_data = get_data_from_categories_csv()
    #attributes_data = get_data_from_attributes_csv()
    
    run_business_sql(business_data)
    #run_hours_sql()
    #run_categories_sql()
    #run_attributes_sql()


        
def run_business_sql(values):
    
    create_table = """
        CREATE TABLE business (
            business_id VARCHAR(30) PRIMARY KEY,
            address VARCHAR(80) NOT NULL,
            open BOOLEAN NOT NULL,
            city VARCHAR(50) NOT NULL,
            review_count INT NOT NULL,
            name VARCHAR(80) NOT NULL,
            neighborhoods VARCHAR(40),
            latitude DECIMAL(8,6) NOT NULL,
            state VARCHAR(2) NOT NULL,
            stars FLOAT NOT NULL,
            longitude DECIMAL(9,6) NOT NULL,
            type VARCHAR(12) NOT NULL
        );
    """
    
    connection = create_server_connection(os.getenv('HOST'), os.getenv(
        'USER'), os.getenv('PASSWORD'), os.getenv('DATABASE'), os.getenv('PORT'))
    
    execute_query(connection, """DROP TABLE IF EXISTS business""")
    execute_query(connection, create_table)
    
    for value in values:
        sql = '''
            INSERT INTO business (business_id, address, open, city, review_count, name, neighborhoods, latitude, state, stars, longitude, type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
            
        val = [(value[0], value[1], bool(value[2]), value[3], value[4], value[5], value[6], value[7], value[8], value[9], value[10], value[11])]
        execute_list_query(connection, sql, val)


def run_hours_sql():
    pass


def run_categories_sql():
    pass


def run_attributes_sql():
    #business_id, attribute, true_or_false
    
    #12312312333,Take-out  , true
    #etc
    
    
    #If it has no attributes, then don't give it an attribute table
    pass

    
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




