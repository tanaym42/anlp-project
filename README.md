# anlp-project
A repository for code to complete the ANLP project. 

Start by installing zstandard (or maybe there's another library for this?)

You can use the command 'pip install zstandard'. 

You can start by testing test-2.py, example-2.py

**Iteration 2:**  

1. Prepare the submissions by extracting them into a csv file, into a location of our choosing. To do this, use the 'submissions-tester.py' file - run the command posted in the comments of the file with input and output modified. It will take a file of '.zst' type and extract the specified fields for the next step. 

2. Now need to run 'comments-id-finder.py' with using the following inputs -- 1. the comments file in zst format, 2. Output CSV, 3. Submissions csv file extracted in the last step. Look at the comments of the file. It currently will only extract 100 pairs. The limit can be found in the bottom of the file. 

The code does need to be cleaned up. It could be more efficient by running them using another single file. Would essentially call the function to create the csv, use it in the next command, delete both, and move to the next in one go. 

Need to figure out how to sample more effectively. Do we sample some every year, which will be very download heavy? Or should we take a few months from every year, in which case we need to change our download strategy. 



