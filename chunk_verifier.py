#!/bin/bash/python3

import hashlib


def compute_original_file_hash(file_path, hash_algorithm='sha256'):
    hash_obj = hashlib.new(hash_algorithm)

    with open(file_path, 'rb') as file:
        while chunk := file.read(8192):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()

def compute_chunks_hash(chunks_ids, hash_algorithm='sha256'):
    # create combined file
    combined_file_name = "combined_file"
    with open(combined_file_name, 'wb') as file:
        for id in chunks_ids:
            with open(id, 'rb') as chunk:
                file.write(chunk.read())
    
    return compute_original_file_hash(combined_file_name)


dir_name = "/home/nandgate/javadocs/cppdocs/sharder/data/"
chunks = ["0_0_0", "1_0_1", "2_0_2", "3_0_3"]
chunks = [dir_name + c for c in chunks]
original_hash = compute_original_file_hash(dir_name+"new_text_file")
downloaded_hash = compute_original_file_hash(dir_name+"downloaded")

print(original_hash)
print(downloaded_hash)
if original_hash == downloaded_hash:
    print("Both files are equal")
else :
    print("Both are different")
