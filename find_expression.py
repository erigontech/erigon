#!/usr/bin/env python3

import argparse
import os
import re

DEFAULT_DIR = '/home/kairat/erigon'

parser = argparse.ArgumentParser(prog='find_expression')

parser.add_argument('--exp', help='expression to search', required=True)
parser.add_argument('--dir', help='search directory', default=DEFAULT_DIR)
parser.add_argument('--ext', help='file extensions to consider (defaults to "go")', default="go")
parser.add_argument('--sub', help='substring, subword, subexpression', action='store_true')

args = vars(parser.parse_args())

EXP = args.get('exp')
DIR = args.get('dir')
EXT: str = args.get('ext')
IS_SUB = args.get('sub')

print("-" * 20)
if EXT == "":
    print("File extensions is not set... will search in all files")
    print("To set file extenstion add flag --ext")
    print("e.g ./find_expression --exp=MyFunction --ext=go,cpp,py,js")
else:
    print("Considering files with ", EXT.split(",")," extansions...")
print(f"Searching for expression {EXP} in default directory {DIR}...")
print("\n")



def print_line(line_no:int, file_path:str):
    new_path = file_path.replace(DIR, "")
    print(f"{new_path}: line {line_no}")


REGEX = re.compile(rf'{EXP}[^\w]')

def find_expression(dir: str, exp: str, exts: list):
    objects = os.listdir(dir)
    
    for obj in objects:
        
        if obj.startswith('.git'):
            continue

        path_to_obj = f"{dir}/{obj}"
        if os.path.isdir(path_to_obj):
            find_expression(path_to_obj, exp, exts)
        
        if os.path.isfile(path_to_obj):
            for _ext in exts:
                if path_to_obj.endswith(f".{_ext}"):
                    with open(path_to_obj, 'r') as f:
                        line = f.readline()
                        count = 1
                        while line:
                            if IS_SUB:
                                if exp in line:
                                    print_line(count, path_to_obj)
                            else:        
                                result = REGEX.search(line)

                                if result is not None:
                                    print_line(count, path_to_obj)                            

                            line = f.readline()
                            count += 1






find_expression(DIR, EXP, EXT.split(","))
print("-" * 20)