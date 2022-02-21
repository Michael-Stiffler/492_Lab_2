import json
import csv
 
 
def main():
    
    data = load_json()
    csv_writer = load_csv()
    write_csv(data, csv_writer)
    
    
def load_json():
    
    with open('business100ValidForm.json') as file:
        json_data = json.load(file)
        
    file.close()
    
    business_data = json_data['Business']
    
    return business_data
    
    
def load_csv():
    
    csv_file = open('table.csv', 'w', newline='')
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
    
if __name__ == "__main__":
    
    main()




