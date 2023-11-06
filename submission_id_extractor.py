import csv

def extract_author_id(csv_file):
    title_to_author_id_dict = {}

    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            prefix_id = ('t3_'+str(row[2]))
            title_to_author_id_dict[prefix_id] = [row[0], row[4]]

    return title_to_author_id_dict

# if __name__ == '__main__':
#     csv_file = input("Enter the path to the CSV file: ")
#     third_elements = extract_third_element(csv_file)
    
#     if third_elements:
#         print("Third elements in each row:")
#         for element in third_elements:
#             print(element)
#     else:
#         print("No valid third elements found in the CSV file.")





