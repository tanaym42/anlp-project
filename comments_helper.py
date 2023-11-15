# Creates a csv file that can be very(!) large when being used to extract information from pusshift zst files

import zstandard
import os
import json
import sys
import csv
from datetime import datetime
import logging.handlers
from submission_extractor import extract_author_id


log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)
			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				yield line, file_handle.tell()

			buffer = lines[-1]
		reader.close()


def extract_comments(comments_path, output_path, submissions_csv, field_list= "link_id,body,author,parent_id"):
	
	"""For a given submissions csv, function will extract all the comments relevant to the submissions already extracted.
	It will place the extracted comments and submissions together in a single file
	
	Parameters: 
	- Path for the zst file containing the comments for a specific month
	- Path for csv file where both submissions and comments will be written
	- Path for the csv file containing the submissions from specific subreddits for a month
	- A list of fields that are important for processing of comments

	Returns:
	- csv file that has combined submissions and comments for a specific month
	- TO ADD - count of the number of lines total as well for each subreddit 
	"""

	input_file_path = comments_path
	output_file_path = output_path
	input_submissions_path = submissions_csv
	fields = field_list.split(",")

	file_size = os.stat(input_file_path).st_size
	file_lines = 0
	lines_processed = 0
	file_bytes_processed = 0
	line = None
	created = None
	bad_lines = 0

	output_file = open(output_file_path, "w", encoding='utf-8', newline="")

	writer = csv.writer(output_file)
	writer.writerow(fields)

	subreddit_counter = {}
	id_tracker = {}

	# Extract list of all the ids and authors corresponding to the submissions in that subreddit, 
	# structured as {"id": ["title", "author", "subreddit"]}
	sub_id_author_dict = extract_author_id(input_submissions_path)

	try:
		#This will read each line in the comments zst file - these can be a lot of lines. 
		for line, file_bytes_processed in read_lines_zst(input_file_path):
			lines_processed += 1

			try:
				obj = json.loads(line)
				output_obj = []
				
				# Obj is structured as PRAWL comment - more details here -> https://praw.readthedocs.io/en/stable/code_overview/models/comment.html

				id_list = list(sub_id_author_dict.keys())
				link_id = str(obj[fields[3]])

				# id_list is the ids of the submissions extracted earlier - link_id is the comment id that links to 'parent'
				if link_id in id_list:
					
					# Checks if the author is the same for the submission and comment - will be ignored
					if str(obj[fields[2]]) == str(sub_id_author_dict[link_id][1]):
						pass

					else:
						# Other random checks to make sure comments are meaningfull - may also want to implement a minimum character count here
						if (str(obj[fields[1]]) == "[deleted]") or (str(obj[fields[1]]) == "[removed]") or ("**" in str(obj[fields[1]])):
							pass
							
						else:
							# Write the details of the line into the csv if all the conditions are met 
							output_obj.append(str(sub_id_author_dict[link_id][0]))
							output_obj.append(str(sub_id_author_dict[link_id][1]))
							output_obj.append(str(obj[fields[1]]).encode("utf-8", errors='replace').decode())


							if link_id in id_tracker.keys():
								if id_tracker[link_id] < 4 and len(str(sub_id_author_dict[link_id][1])) > 50:
									
									id_tracker[link_id] = 1
									writer.writerow(output_obj)
									file_lines += 1

									subm_subreddit = sub_id_author_dict[link_id][2]

									if subm_subreddit in sub_id_author_dict.keys():
										subreddit_counter[subm_subreddit] += 1
							
									else:
										subreddit_counter[subm_subreddit] = 1
								
								else:
									pass

							else:
								id_tracker[link_id] = 1
								writer.writerow(output_obj)
								file_lines += 1

								subm_subreddit = sub_id_author_dict[link_id][2]
								if subm_subreddit in sub_id_author_dict.keys():
									subreddit_counter[subm_subreddit] += 1
							
								else:
									subreddit_counter[subm_subreddit] = 1


	
				created = datetime.utcfromtimestamp(int(obj['created_utc']))
			except json.JSONDecodeError as err:
				bad_lines += 1
			
			# This will control the speed at which progress is printed
			if lines_processed % 100000 == 0:
				log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')}: Lines_processed : {lines_processed:,} ")

			# Limit to the number of lines that are to be processed before the program is terminated
			if lines_processed == 1000000:
				break
			
			# Limit to the number of output lines that are to be written from any specific month from all subreddits 
			if file_lines == 500:
				log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {bad_lines:,} : {(file_bytes_processed / file_size) * 100:.0f}%")
				break
	except KeyError as err:
		log.info(f"Object has no key: {err}")
		log.info(line)
	except Exception as err:
		log.info(err)
		log.info(line)

	output_file.close()
	log.info(f"Complete : {file_lines:,} : {bad_lines:,}")
	log.info(f"Lines_processed in comments : {lines_processed:,} ")
	print(subreddit_counter)
	


# field_inputs = "link_id,body,author,parent_id"
# extract_comments('reddit/comments/RC_2019-10.zst', 'trial_combined_3.csv','trial_submissions_2.csv', field_inputs)