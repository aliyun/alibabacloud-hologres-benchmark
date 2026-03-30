#!/usr/bin/env python3
import sys
import json
import csv
import os
import glob


def convert_file(input_file, output_file, start_id):
    with open(input_file, 'r', encoding='utf-8') as infile, \
            open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        csv_writer = csv.writer(outfile)
        record_id = start_id

        for line in infile:
            line = line.strip()
            if line:
                try:
                    obj = json.loads(line)
                    row = [record_id] + list(obj.values())
                    csv_writer.writerow(row)
                    record_id += 1
                except json.JSONDecodeError:
                    continue

        return record_id


def main():
    if len(sys.argv) != 3:
        print("用法: python ndjson_to_csv.py <输入文件夹> <输出文件夹>", file=sys.stderr)
        sys.exit(1)

    input_folder = sys.argv[1]
    output_folder = sys.argv[2]

    os.makedirs(output_folder, exist_ok=True)

    files = sorted(glob.glob(os.path.join(input_folder, "*.json")))

    current_id = 1
    for input_file in files:
        filename = os.path.basename(input_file)
        name_without_ext = os.path.splitext(filename)[0]
        output_file = os.path.join(output_folder, f"{name_without_ext}.csv")
        current_id = convert_file(input_file, output_file, current_id)


if __name__ == '__main__':
    main()

