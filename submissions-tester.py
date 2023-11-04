# this converts a zst file to csv
#
# it's important to note that the resulting file will likely be quite large
# and you probably won't be able to open it in excel or another csv reader
#
# arguments are inputfile, outputfile, fields
# call this like
#TANAY CALL THIS IN THE TERMINAL LIKE THIS
# python3 test-4.py RC_2005-12.zst output.csv controversiality,body,subreddit_id,link_id,stickied,subreddit,score
# python3 test-4.py RC_2006-01.zst output-3.csv body,subreddit,score
# reddit/submissions/RS_2019-10.zst
# python3 submissions-tester.py reddit/submissions/RS_2019-10.zst output-submissions-askwomen.csv title,subreddit,id

# {"all_awardings":[],"allow_live_comments":false,"archived":false,"author":"Independent-Ad7276","author_created_utc":1596964740,"author_flair_background_color":null,"author_flair_css_class":null,"author_flair_richtext":[],"author_flair_template_id":null,"author_flair_text":null,"author_flair_text_color":null,"author_flair_type":"text","author_fullname":"t2_7mkth3e9","author_patreon_flair":false,"author_premium":false,"awarders":[],"banned_by":null,"can_gild":true,"can_mod_post":false,"category":null,"content_categories":null,"contest_mode":false,"created_utc":1667261166,"discussion_type":null,"distinguished":null,"domain":"self.AskWomen","edited":false,"gilded":0,"gildings":{},"hidden":false,"hide_score":false,"id":"yit7li","is_created_from_ads_ui":false,"is_crosspostable":false,"is_meta":false,"is_original_content":false,"is_reddit_media_domain":false,"is_robot_indexable":false,"is_self":true,"is_video":false,"link_flair_background_color":"","link_flair_css_class":null,"link_flair_richtext":[],"link_flair_text":null,"link_flair_text_color":"dark","link_flair_type":"text","locked":true,"media":null,"media_embed":{},"media_only":false,"name":"t3_yit7li","no_follow":true,"num_comments":1,"num_crossposts":0,"over_18":false,"parent_whitelist_status":"all_ads","permalink":"\/r\/AskWomen\/comments\/yit7li\/what_are_your_thoughts_about_grown_up_men_that\/","pinned":false,"pwls":6,"quarantine":false,"removed_by":null,"removed_by_category":"moderator","retrieved_on":1668014925,"score":1,"secure_media":null,"secure_media_embed":{},"selftext":"","send_replies":true,"spoiler":false,"stickied":false,"subreddit":"AskWomen","subreddit_id":"t5_2rxrw","subreddit_name_prefixed":"r\/AskWomen","subreddit_subscribers":4419505,"subreddit_type":"public","suggested_sort":"top","thumbnail":"default","thumbnail_height":null,"thumbnail_width":null,"title":"What are your thoughts about grown up men that still wear baseball hats all the time? (indoors, at night, etc.)","top_awarded_type":null,"total_awards_received":0,"treatment_tags":[],"upvote_ratio":1.0,"url":"https:\/\/www.reddit.com\/r\/AskWomen\/comments\/yit7li\/what_are_your_thoughts_about_grown_up_men_that\/","view_count":null,"whitelist_status":"all_ads","wls":6}

import zstandard
import os
import json
import sys
import csv
from datetime import datetime
import logging.handlers


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
	fields = sys.argv[3].split(",")

	file_size = os.stat(input_file_path).st_size
	file_lines = 0
	file_bytes_processed = 0
	line = None
	created = None
	bad_lines = 0
	output_file = open(output_file_path, "w", encoding='utf-8', newline="")
	writer = csv.writer(output_file)
	writer.writerow(fields)
	try:
		for line, file_bytes_processed in read_lines_zst(input_file_path):
			try:
				obj = json.loads(line)
				output_obj = []

				# fields = [title, subreddit, id]
				subreddit_name = str(obj[fields[1]]).encode("utf-8", errors='replace').decode()

				if subreddit_name == "AskWomen":
					output_obj.append(str(obj[fields[0]]).encode("utf-8", errors = 'replace').decode())
					output_obj.append(str(obj[fields[1]]).encode("utf-8", errors = 'replace').decode())
					output_obj.append(str(obj[fields[2]]).encode("utf-8", errors = 'replace').decode())
					writer.writerow(output_obj)

				created = datetime.utcfromtimestamp(int(obj['created_utc']))
			except json.JSONDecodeError as err:
				bad_lines += 1
			file_lines += 1
			if file_lines % 100000 == 0:
				log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {bad_lines:,} : {(file_bytes_processed / file_size) * 100:.0f}%")
	except KeyError as err:
		log.info(f"Object has no key: {err}")
		log.info(line)
	except Exception as err:
		log.info(err)
		log.info(line)

	output_file.close()
	log.info(f"Complete : {file_lines:,} : {bad_lines:,}")
