# this converts a zst file to csv
#
# it's important to note that the resulting file will likely be quite large
# and you probably won't be able to open it in excel or another csv reader
#
# arguments are inputfile, outputfile, fields
# call this like
#TANAY CALL THIS IN THE TERMINAL LIKE THIS
# python3 comments_converter.py AskWomen_comments.zst askwomen_test.csv body,subreddit,parent_id,created_utc

#ISI THIS IS THE NEW COMMAND, INPUT AND OUTPUT TO BE CHANGED
# fields = [link_id,body,author,parent_id]
# python3 comments-id-finder.py reddit/comments/RC_2019-10.zst sc_combined_test_two.csv output-submissions-askwomen-trial.csv  link_id,body,author,parent_id

import zstandard
import os
import json
import sys
import csv
from datetime import datetime
import logging.handlers
from submission_id_extractor import extract_author_id


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


if __name__ == "__main__":
	input_file_path = sys.argv[1]
	output_file_path = sys.argv[2]
	input_submissions_path = sys.argv[3]
	fields = sys.argv[4].split(",")

	file_size = os.stat(input_file_path).st_size
	file_lines = 0
	file_bytes_processed = 0
	line = None
	created = None
	bad_lines = 0
	output_file = open(output_file_path, "w", encoding='utf-8', newline="")
	writer = csv.writer(output_file)
	writer.writerow(fields)

	# Extract list of all the ids and authors corresponding to the submissions in that subreddit, structured as {"id": ["title", "author"]}
	sub_id_author_dict = extract_author_id(input_submissions_path)

	try:
		for line, file_bytes_processed in read_lines_zst(input_file_path):
			try:
				obj = json.loads(line)
				output_obj = []
				# we are playing with this part of the code
				# print(str(obj[fields[0]]))
				# print(sub_id_author_dict.keys()[0:5])
				id_list = list(sub_id_author_dict.keys())

				# print("I am getting here...")
				# print(id_list[0:5])
				# print(str(obj[fields[0]]))

				for link_id in id_list:
					if str(obj[fields[3]]) == link_id:
						# print(str(obj[fields[3]]))
						# print(link_id)
						# print("I found one, it seems...")
						# output_obj.append(str(obj[fields[0]]).encode("utf-8", errors='replace').decode())

						# if str(obj[fields[0]]) == str(obj[fields[3]]):
						# 	print("Failed the parent_id check")
						# 	pass

						if str(obj[fields[2]]) == str(sub_id_author_dict[link_id][1]):
							# print("Failed the author check")
							pass

						else:
							# print("Adding to csv")
							output_obj.append(str(sub_id_author_dict[link_id][0]))
							output_obj.append(str(obj[fields[1]]).encode("utf-8", errors='replace').decode())
							writer.writerow(output_obj)
							file_lines += 1
	
				created = datetime.utcfromtimestamp(int(obj['created_utc']))
			except json.JSONDecodeError as err:
				bad_lines += 1
			
			if file_lines == 100:
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
