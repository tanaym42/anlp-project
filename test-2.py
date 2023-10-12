import zstandard as zstd
import json

print(f"ZST File Path: {'RC_2005-12.zst'}")
print(f"JSON File Path: {'output.json'}")

def zst_to_json(zst_file_path, json_file_path):
    with open(zst_file_path, 'rb') as zst_file:
        decompressor = zstd.ZstdDecompressor()
        with decompressor.stream_reader(zst_file) as reader:
            with open(json_file_path, 'wb') as json_file:
                print(type(reader))
                for chunk in reader:
                    # Write the raw chunk to the JSON file
                    json_file.write(chunk)

# Replace 'input.zst' and 'output.json' with your file paths
zst_to_json('RC_2005-12.zst', 'output.json')