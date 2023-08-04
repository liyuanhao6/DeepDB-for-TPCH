import os
import re


def tbl_to_csv(tbl_file_path, csv_file_path):
    with open(tbl_file_path, 'r') as in_file, open(csv_file_path, 'w') as out_file:
        for (i, line) in enumerate(in_file):
            if i >= 1e10:
                break
            line = line.strip().rstrip('|')
            pattern = r'\d{4}-\d{2}-\d{2}'
            matches = re.findall(pattern, line)
            for match in matches:
                replacement = re.sub(r'(\d{4})-(\d{2})-(\d{2})', r'\1\2\3', match)
                line = line.replace(match, replacement)
            out_file.write(line + '\n')


# the directory containing the .tbl files
tbl_directory = 'Data/tpch_tbl'
csv_directory = 'Data/tpch'

# iterate over all files in the directory
for filename in os.listdir(tbl_directory):
    # check if this is a .tbl file
    if filename.endswith('.tbl'):
        tbl_file_path = os.path.join(tbl_directory, filename)
        csv_file_path = os.path.join(csv_directory, filename.replace('.tbl', '.csv'))
        tbl_to_csv(tbl_file_path, csv_file_path)
