import csv

def extract_third_element(csv_file):
    third_elements = {}

    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) >= 3:  # Make sure the row has at least 3 elements
                third_elements[row[2]] = row[0]

    return third_elements

# if __name__ == '__main__':
#     csv_file = input("Enter the path to the CSV file: ")
#     third_elements = extract_third_element(csv_file)
    
#     if third_elements:
#         print("Third elements in each row:")
#         for element in third_elements:
#             print(element)
#     else:
#         print("No valid third elements found in the CSV file.")





