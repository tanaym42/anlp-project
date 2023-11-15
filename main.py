# python3 main.py reddit/submissions/RS_2019-10.zst reddit/comments/RC_2019-10.zst output/2019-10.csv

from submission_helper import extract_submissions
from comments_helper import extract_comments
import sys
import os

if __name__ == "__main__":

    if len(sys.argv) != 4:
        # Need to fix the below
        print("Usage: python script.py <arg1> <arg2>") 
        sys.exit(1)

    submissions_input = sys.argv[1]
    comments_input = sys.argv[2]
    combined_output = sys.argv[3]

    intermediate_folder = 'intermediate'
    intermediate_csv = os.path.join(intermediate_folder, 'temp.csv')

    output_path = os.path.dirname(combined_output)
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    if not os.path.exists(intermediate_folder):
        os.makedirs(intermediate_folder)
    
    extract_submissions(submissions_input, intermediate_csv)
    extract_comments(comments_input, combined_output, intermediate_csv)



