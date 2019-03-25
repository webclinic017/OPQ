

'''
Project utility funcitons.

'''

import os
import time

import pandas as pd


def write_log(msg, log_filename):
    '''
    Append a log message to the log file.
    '''
    
    full_msg = time.strftime("%Y%m%d %H:%M:%S") + '\t' + msg
    if not os.path.isfile(log_filename):
        create_dir_and_file(log_filename)
    with open(log_filename, 'a') as F:
        F.write(full_msg + '\n')
    print(full_msg)



def create_dir_and_file(filepath, overwrite=False):
    '''
    If the file doesn't exist, create it and its folder(s).
    '''

    path_dir, filename = os.path.split(filepath)
    path_stack = []
    while path_dir and not os.path.isdir(path_dir):
        path_dir, _dir = os.path.split(path_dir)
        path_stack.append(_dir)
    while path_stack:
        path_dir = os.path.join(path_dir, path_stack.pop())
        os.mkdir(path_dir)
    if not os.path.isfile(filename) or overwrite:
        with open(os.path.join(path_dir, filename), 'w') as F:
            pass


def load_config(filename="config.txt"):
    '''
    Load the project config file.
    '''
    
    config = {}
    with open(filename) as F:
        for line in F:
            param, value = line.strip().split('\t')
            config[param] = value
    return config



def load_stock_data(folder, stock_codes=None):
    '''
    Load the stock data.
    The returned object stores is a dict, with key being the stock code,
    and each value (data of a specific stock) being a pandas DataFrame object.
    '''

    if stock_codes is None:
        filenames = os.listdir(folder)
    else:
        filenames = [code + '.csv' for code in stock_codes]
    
    data = {}
    for fname in filenames:
        stock_code = get_stock_code(fname)
        df = pd.read_csv(os.path.join(folder, fname))
        data[stock_code] = df.set_index("Date")

    return data


def get_stock_code(filename):
    '''
    Get stock code from a filename.
    "DLTR.OQ.csv" -> "DLTR.OQ"
    '''
    
    return '.'.join(filename.split('.')[:-1])


def merge_output(input_folder, output_file):
    '''
    Merge output files into one.
    '''

    create_dir_and_file(output_file)
    out_df = pd.DataFrame()
    for fname in os.listdir(input_folder):
        fname = os.path.join(input_folder, fname)
        print("Merging %s" % fname)
        try:
            df = pd.read_csv(fname)
        except:
            continue
        out_df = pd.concat([out_df, df])  
    out_df = out_df.drop_duplicates(subset='index', keep='first')

    out_df.to_csv(output_file, index=False)
    print("Merged complete")


