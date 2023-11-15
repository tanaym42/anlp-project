# python3 main.py reddit/submissions/RS_2019-10.zst reddit/comments/RC_2019-10.zst output/2019-10.csv

from submission_helper import extract_submissions
from comments_helper import extract_comments
import sys

if __name__ == "main":

    if len(sys.argv) != 4:
        # Need to fix the below
        print("Usage: python script.py <arg1> <arg2>") 

    submissions_input = sys.argv[1]
    comments_input = sys.argv[2]
    combined_output = sys.argv[3]

    intermediate_csv = 'intermediate/temp.csv'
    
    extract_submissions(submissions_input, intermediate_csv)
    extract_comments(comments_input, combined_output, intermediate_csv)



