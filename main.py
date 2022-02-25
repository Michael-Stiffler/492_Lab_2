import os
import json
import csv
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import copy


def main():

    data = load_json()
    data = clean_data(data)
    write_csv(data)

    load_dotenv()
    run_sql()


def load_json():

    with open('business100ValidForm.json') as file:
        json_data = json.load(file)

    file.close()

    business_data = json_data['Business']

    return business_data


def clean_data(data_array):

    all_categories = {}
    all_attributes = {}

    for json in data_array:
        new_list = json['categories']

        for item in new_list:
            new_item = item
            if "'" in new_item:
                new_item = new_item.replace("\'", "")
            if new_item not in all_categories:
                all_categories[new_item] = 0

        new_list = json['attributes']

        for item in new_list:

            item_to_add = ""

            if type(new_list[item]) is dict:
                for attr in new_list[item]:
                    item_to_add = item + " " + attr
                    if item_to_add not in all_attributes:
                        all_attributes[item_to_add] = None
            else:
                if item not in all_attributes:
                    all_attributes[item] = None

    cleaned_data = data_array

    for json in cleaned_data:

        # cleans the hours
        json["hours"] = format_hours(json["hours"])

        # cleans the categories
        new_list = json['categories']
        all_categories_copy = copy.deepcopy(all_categories)
        for item in new_list:
            new_item = item
            if "'" in new_item:
                new_item = new_item.replace("\'", "")
            all_categories_copy[new_item] = 1

        json['categories'] = all_categories_copy

        # cleans the attributes
        new_list = json['attributes']
        all_attributes_copy = copy.deepcopy(all_attributes)
        for item in new_list:
            if type(new_list[item]) is dict:
                for item_in_sublist in new_list[item]:
                    key = item + " " + item_in_sublist
                    value = new_list[item][item_in_sublist]
                    if value == True:
                        value = 1
                    elif value == False:
                        value = 0
                    all_attributes_copy[key] = value
            else:
                if new_list[item] == True:
                    new_list[item] = 1
                elif new_list[item] == False:
                    new_list[item] = 0
                all_attributes_copy[item] = new_list[item]


        json['attributes'] = all_attributes_copy

        # cleans the neighbors
        if str(json["neighborhoods"]) == "[]":
            json["neighborhoods"] = None
        else:
            string = json["neighborhoods"]
            json["neighborhoods"] = str(json["neighborhoods"]).strip("[']")


    return cleaned_data


def format_hours(hours):

    new_format_of_hours = {
        "sunday_open": None,
        "sunday_close": None,
        "monday_open": None,
        "monday_close": None,
        "tuesday_open": None,
        "tuesday_close": None,
        "wednesday_open": None,
        "wednesday_close": None,
        "thursday_open": None,
        "thursday_close": None,
        "friday_open": None,
        "friday_close": None,
        "saturday_open": None,
        "saturday_close": None
    }

    if 'Sunday' in hours:
        new_format_of_hours['sunday_open'] = str(hours['Sunday']['open'])
        new_format_of_hours['sunday_close'] = str(hours['Sunday']['close'])
    if 'Monday' in hours:
        new_format_of_hours['monday_open'] = str(hours['Monday']['open'])
        new_format_of_hours['monday_close'] = str(hours['Monday']['close'])
    if 'Tuesday' in hours:
        new_format_of_hours['tuesday_open'] = str(hours['Tuesday']['open'])
        new_format_of_hours['tuesday_close'] = str(hours['Tuesday']['close'])
    if 'Wednesday' in hours:
        new_format_of_hours['wednesday_open'] = str(hours['Wednesday']['open'])
        new_format_of_hours['wednesday_close'] = str(
            hours['Wednesday']['close'])
    if 'Thursday' in hours:
        new_format_of_hours['thursday_open'] = str(hours['Thursday']['open'])
        new_format_of_hours['thursday_close'] = str(hours['Thursday']['close'])
    if 'Friday' in hours:
        new_format_of_hours['friday_open'] = str(hours['Friday']['open'])
        new_format_of_hours['friday_close'] = str(hours['Friday']['close'])
    if 'Saturday' in hours:
        new_format_of_hours['saturday_open'] = str(hours['Saturday']['open'])
        new_format_of_hours['saturday_close'] = str(hours['Saturday']['close'])

    return new_format_of_hours


def load_csv(filename):

    csv_file = open(filename, 'w', newline='')
    csv_writer = csv.writer(csv_file)
    return csv_writer


def write_csv(data):

    business_data_writer = load_csv('csv/business_data.csv')
    hours_data_writer = load_csv('csv/hours_data.csv')
    categories_data_writer = load_csv('csv/categories_data.csv')
    attributes_data_writer = load_csv('csv/attributes_data.csv')

    header_row = True

    for json in data:
        if header_row:
            header = json.keys()

            header = list(header)

            list_to_write = [header[0], header[1], header[3], header[5], header[6],
                             header[7], header[8], header[9], header[10], header[11], header[12], header[14]]
            business_data_writer.writerow(list_to_write)

            list_to_write = [i for i in json['hours']]
            list_to_write.insert(0, header[0])
            hours_data_writer.writerow(list_to_write)

            list_to_write = [i for i in json['categories']]
            list_to_write.insert(0, header[0])
            categories_data_writer.writerow(list_to_write)

            list_to_write = [i for i in json['attributes']]
            list_to_write.insert(0, header[0])
            attributes_data_writer.writerow(list_to_write)

            header_row = False

        if json['open'] == True:
            json['open'] = 1
        elif json['open'] == False:
            json['open'] = 0
            
        list_to_write = [json['business_id'], json['full_address'], json['open'], json['city'], json['review_count'],
                         json['name'], json['neighborhoods'], json['longitude'], json['state'], json['stars'], json['latitude'], json['type']]
        business_data_writer.writerow(list_to_write)

        list_to_write = [i for i in json['hours'].values()]
        list_to_write.insert(0, json['business_id'])
        hours_data_writer.writerow(list_to_write)

        list_to_write = [i for i in json['categories'].values()]
        list_to_write.insert(0, json['business_id'])
        categories_data_writer.writerow(list_to_write)

        list_to_write = [i for i in json['attributes'].values()]
        list_to_write.insert(0, json['business_id'])
        attributes_data_writer.writerow(list_to_write)


def get_data_from_business_csv():
    csv_file = open('csv/business_data.csv')
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
    csv_file = open('csv/hours_data.csv')
    csv_reader = csv.reader(csv_file)

    values = []
    header = True

    for row in csv_reader:
        if header:
            header = False
            continue
        else: 
            new_list = []
            for element in row:
                if len(element) == 0:
                    element = None
                    new_list.append(element)
                else:
                    new_list.append(element)
        values.append(new_list)
        
    return values


def get_data_from_categories_csv():
    csv_file = open('csv/categories_data.csv')
    csv_reader = csv.reader(csv_file)

    values = []
    header = True

    for row in csv_reader:
        if header:
            header = False
            continue
        else:
            values.append(row)

    return values


def get_data_from_attributes_csv():
    csv_file = open('csv/attributes_data.csv')
    csv_reader = csv.reader(csv_file)

    header = True
    values = []
    new_list = []

    for row in csv_reader:
        if header:
            header = False
            continue
        else: 
            new_list = []
            for element in row:
                if len(element) == 0:
                    element = None
                    new_list.append(element)
                else:
                    new_list.append(element)
        values.append(new_list)
        
    return values


def run_sql():

    business_data = get_data_from_business_csv()
    hours_data = get_data_from_hours_csv()
    categories_data = get_data_from_categories_csv()
    attributes_data = get_data_from_attributes_csv()

    run_business_sql(business_data)
    run_hours_sql(hours_data)
    run_categories_sql(categories_data)
    run_attributes_sql(attributes_data)


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

        val = [(value[0], value[1], value[2], value[3], value[4], value[5],
                value[6], value[7], value[8], value[9], value[10], value[11])]
        execute_list_query(connection, sql, val)


def run_hours_sql(values):
    
    create_table = """
    CREATE TABLE business_hours (
        `business_id` varchar(255) NOT NULL,
        `sunday_open` varchar(255),
        `sunday_close` varchar(255),
        `monday_open` varchar(255),
        `monday_close` varchar(255),
        `tuesday_open` varchar(255),
        `tuesday_close` varchar(255),
        `wednesday_open` varchar(255),
        `wednesday_close` varchar(255),
        `thursday_open` varchar(255),
        `thursday_close` varchar(255),
        `friday_open` varchar(255),
        `friday_close` varchar(255),
        `saturday_open` varchar(255),
        `saturday_close` varchar(255),
        PRIMARY KEY (`business_id`)
        ); 
    """
    
    connection = create_server_connection(os.getenv('HOST'), os.getenv(
        'USER'), os.getenv('PASSWORD'), os.getenv('DATABASE'), os.getenv('PORT'))

    execute_query(connection, """DROP TABLE IF EXISTS business_hours""")
    execute_query(connection, create_table)
    
    for value in values:
        sql = '''
        INSERT INTO business_hours (`business_id`, `sunday_open`, `sunday_close`, `monday_open`, `monday_close`, `tuesday_open`, `tuesday_close`, `wednesday_open`, `wednesday_close`, `thursday_open`, `thursday_close`, `friday_open`, `friday_close`, `saturday_open`, `saturday_close`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        
        temp_val = [i for i in value]
        val = [tuple(temp_val)]
        execute_list_query(connection, sql, val)


def run_categories_sql(values):
    
    create_table = """
    CREATE TABLE `business_categories` ( `business_id` varchar(255) NOT NULL, `Fast Food` tinyint, `Restaurants` tinyint, 
    `Nightlife` tinyint, `Auto Repair` tinyint, `Automotive` tinyint, `Active Life` tinyint, `Mini Golf` tinyint, 
    `Golf` tinyint, `Shopping` tinyint, `Home Services` tinyint, `Internet Service Providers` tinyint, 
    `Mobile Phones` tinyint, `Professional Services` tinyint, `Electronics` tinyint, `Bars` tinyint, 
    `American (New)` tinyint, `Lounges` tinyint, `Trainers` tinyint, `Fitness & Instruction` tinyint, 
    `American (Traditional)` tinyint, `Tires` tinyint, `Contractors` tinyint, `Veterinarians` tinyint, 
    `Pets` tinyint, `Libraries` tinyint, `Public Services & Government` tinyint, `Auto Parts & Supplies` tinyint, 
    `Burgers` tinyint, `Breakfast & Brunch` tinyint, `Food` tinyint, `Grocery` tinyint, `Local Services` tinyint, 
    `Dry Cleaning & Laundry` tinyint, `Sewing & Alterations` tinyint, `Gas & Service Stations` tinyint, 
    `Sandwiches` tinyint, `Cafes` tinyint, `Hotels & Travel` tinyint, `Event Planning & Services` tinyint, 
    `Hotels` tinyint, `Pubs` tinyint, `Irish` tinyint, `Body Shops` tinyint, `Health & Medical` tinyint, 
    `Dentists` tinyint, `General Dentistry` tinyint, `Chinese` tinyint, `Gyms` tinyint, `Italian` tinyint, 
    `Comfort Food` tinyint, `Caterers` tinyint, `Pizza` tinyint, `Coffee & Tea` tinyint, `Arts & Crafts` tinyint, 
    `Knitting Supplies` tinyint, `Hobby Shops` tinyint, `Gift Shops` tinyint, `Flowers & Gifts` tinyint, 
    `Home Decor` tinyint, `Home & Garden` tinyint, `Department Stores` tinyint, `Fashion` tinyint, 
    `Building Supplies` tinyint, `Hardware Stores` tinyint, `Nurseries & Gardening` tinyint, `Arts & Entertainment` tinyint,
    `Arcades` tinyint, `Pet Services` tinyint, `Pet Boarding/Pet Sitting` tinyint, `Breweries` tinyint,
    `Comedy Clubs` tinyint, `Books, Mags, Music & Video` tinyint, `Bookstores` tinyint, `Swimming Pools` tinyint,
    `Mattresses` tinyint, `Airport Shuttles` tinyint, `Limos` tinyint, `Transportation` tinyint,
    `Gluten-Free` tinyint, `Cosmetics & Beauty Supply` tinyint, `Beauty & Spas` tinyint, `Lingerie` tinyint,
    `Womens Clothing` tinyint, `Shoe Stores` tinyint, `Asian Fusion` tinyint, `Soup` tinyint,
    `Salad` tinyint, `Mens Clothing` tinyint, `Formal Wear` tinyint, `Diners` tinyint, `Discount Store` tinyint,
    `Seafood` tinyint, `Childrens Clothing` tinyint, `Sporting Goods` tinyint, `Sports Wear` tinyint,
    `Bikes` tinyint, `Bakeries` tinyint, `Desserts` tinyint, `Tattoo` tinyint, `Piercing` tinyint,
    `Sports Bars` tinyint, `Japanese` tinyint, `Specialty Food` tinyint, `Meat Shops` tinyint, `Florists` tinyint,
    `Oil Change Stations` tinyint, PRIMARY KEY (`business_id`)); 
    """
    
    connection = create_server_connection(os.getenv('HOST'), os.getenv(
        'USER'), os.getenv('PASSWORD'), os.getenv('DATABASE'), os.getenv('PORT'))

    execute_query(connection, """DROP TABLE IF EXISTS business_categories""")
    execute_query(connection, create_table)
    
    for value in values:
        sql = '''
        INSERT INTO business_categories (`business_id`, `Fast Food`, `Restaurants`, `Nightlife`, `Auto Repair`, `Automotive`, `Active Life`, `Mini Golf`, `Golf`, `Shopping`, `Home Services`, `Internet Service Providers`, `Mobile Phones`, `Professional Services`, `Electronics`, `Bars`, `American (New)`, `Lounges`, `Trainers`, `Fitness & Instruction`, `American (Traditional)`, `Tires`, `Contractors`, `Veterinarians`, `Pets`, `Libraries`, `Public Services & Government`, `Auto Parts & Supplies`, `Burgers`, `Breakfast & Brunch`, `Food`, `Grocery`, `Local Services`, `Dry Cleaning & Laundry`, `Sewing & Alterations`, `Gas & Service Stations`, `Sandwiches`, `Cafes`, `Hotels & Travel`, `Event Planning & Services`, `Hotels`, `Pubs`, `Irish`, `Body Shops`, `Health & Medical`, `Dentists`, `General Dentistry`, `Chinese`, `Gyms`, `Italian`, `Comfort Food`, `Caterers`, `Pizza`, `Coffee & Tea`, `Arts & Crafts`, `Knitting Supplies`, `Hobby Shops`, `Gift Shops`, `Flowers & Gifts`, `Home Decor`, `Home & Garden`, `Department Stores`, `Fashion`, `Building Supplies`, `Hardware Stores`, `Nurseries & Gardening`, `Arts & Entertainment`, `Arcades`, `Pet Services`, `Pet Boarding/Pet Sitting`, `Breweries`, `Comedy Clubs`, `Books, Mags, Music & Video`, `Bookstores`, `Swimming Pools`, `Mattresses`, `Airport Shuttles`, `Limos`, `Transportation`, `Gluten-Free`, `Cosmetics & Beauty Supply`, `Beauty & Spas`, `Lingerie`, `Womens Clothing`, `Shoe Stores`, `Asian Fusion`, `Soup`, `Salad`, `Mens Clothing`, `Formal Wear`, `Diners`, `Discount Store`, `Seafood`, `Childrens Clothing`, `Sporting Goods`, `Sports Wear`, `Bikes`, `Bakeries`, `Desserts`, `Tattoo`, `Piercing`, `Sports Bars`, `Japanese`, `Specialty Food`, `Meat Shops`, `Florists`, `Oil Change Stations`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        
        temp_val = [i for i in value]
        val = [tuple(temp_val)]
        execute_list_query(connection, sql, val)


def run_attributes_sql(values):
    
    create_table = """ 
    CREATE TABLE business_attributes (
        business_id varchar(255) NOT NULL,
        `Take-out` tinyint ,
        `Drive-Thru` tinyint ,
        `Good For dessert` tinyint ,
        `Good For latenight` tinyint ,
        `Good For lunch` tinyint ,
        `Good For dinner` tinyint ,
        `Good For brunch` tinyint ,
        `Good For breakfast` tinyint ,
        `Caters` tinyint ,
        `Noise Level` varchar(255) ,
        `Takes Reservations` tinyint ,
        `Delivery` tinyint ,
        `Ambience romantic` tinyint ,
        `Ambience intimate` tinyint ,
        `Ambience classy` tinyint ,
        `Ambience hipster` tinyint ,
        `Ambience divey` tinyint ,
        `Ambience touristy` tinyint ,
        `Ambience trendy` tinyint ,
        `Ambience upscale` tinyint ,
        `Ambience casual` tinyint ,
        `Parking garage` tinyint ,
        `Parking street` tinyint ,
        `Parking validated` tinyint ,
        `Parking lot` tinyint ,
        `Parking valet` tinyint ,
        `Has TV` tinyint ,
        `Outdoor Seating` tinyint ,
        `Attire` varchar(255) ,
        `Alcohol` varchar(255) ,
        `Waiter Service` tinyint ,
        `Accepts Credit Cards` tinyint ,
        `Good for Kids` tinyint ,
        `Good For Groups` tinyint ,
        `Price Range` int ,
        `Happy Hour` tinyint ,
        `Good For Dancing` tinyint ,
        `Coat Check` tinyint ,
        `Smoking` varchar(255) ,
        `Wi-Fi` varchar(255) ,
        `Music dj` tinyint ,
        `Wheelchair Accessible` tinyint ,
        `Dogs Allowed` tinyint ,
        `BYOB` tinyint ,
        `Corkage` tinyint ,
        `BYOB/Corkage` varchar(255) ,
        `Order at Counter` tinyint ,
        `Music background_music` tinyint ,
        `Music jukebox` tinyint ,
        `Music live` tinyint ,
        `Music video` tinyint ,
        `Music karaoke` tinyint ,
        `By Appointment Only` tinyint ,
        PRIMARY KEY (`business_id`)
        );
    """
    connection = create_server_connection(os.getenv('HOST'), os.getenv(
        'USER'), os.getenv('PASSWORD'), os.getenv('DATABASE'), os.getenv('PORT'))

    execute_query(connection, """DROP TABLE IF EXISTS business_attributes""")
    execute_query(connection, create_table)
    
    for value in values:
        sql = '''
        INSERT INTO business_attributes (`business_id`, `Take-out`, `Drive-Thru`, `Good For dessert`, `Good For latenight`, `Good For lunch`, `Good For dinner`, `Good For brunch`, `Good For breakfast`, `Caters`, `Noise Level`, `Takes Reservations`, `Delivery`, `Ambience romantic`, `Ambience intimate`, `Ambience classy`, `Ambience hipster`, `Ambience divey`, `Ambience touristy`, `Ambience trendy`, `Ambience upscale`, `Ambience casual`, `Parking garage`, `Parking street`, `Parking validated`, `Parking lot`, `Parking valet`, `Has TV`, `Outdoor Seating`, `Attire`, `Alcohol`, `Waiter Service`, `Accepts Credit Cards`, `Good for Kids`, `Good For Groups`, `Price Range`, `Happy Hour`, `Good For Dancing`, `Coat Check`, `Smoking`, `Wi-Fi`, `Music dj`, `Wheelchair Accessible`, `Dogs Allowed`, `BYOB`, `Corkage`, `BYOB/Corkage`, `Order at Counter`, `Music background_music`, `Music jukebox`, `Music live`, `Music video`, `Music karaoke`, `By Appointment Only`)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s)
        '''

        temp_val = [i for i in value]
        val = [tuple(temp_val)]
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
