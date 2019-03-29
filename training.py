
'''
Training the pair trading system by calculating correlations
between all combinations of stock pairs.

'''


import os
import time
from multiprocessing import Process, Queue

import pandas as pd

import calc
from util import *


# Control the metrics to calculate. Must match the names defined in "calc.py"
METRICS = ["CoInt", "PCC_log", "SSD_SMA3"]



def generate_jobs(stock_data_folder):
    '''
    Generate all the jobs.
    Each job has an id and two stock codes to represent a pair.
    '''
    
    stock_codes = [get_stock_code(fname) for fname in os.listdir(stock_data_folder)]
    stock_codes.sort()
    num_stocks = len(stock_codes)
    jobs = []
    job_index = 0
    for i in range(num_stocks):
        for j in range(i + 1, num_stocks):
            jobs.append((str(job_index), stock_codes[i], stock_codes[j]))
            job_index += 1
    return jobs


def generate_new_output_file(output_folder):
    '''
    Generate a new output file. Return the filename (with path).
    '''
    
    num_prev_runs = 0
    while os.path.isfile(os.path.join(output_folder, "out_%s.csv" % num_prev_runs)):
        num_prev_runs += 1
    fname = os.path.join(output_folder, "out_%s.csv" % num_prev_runs)
    create_dir_and_file(fname)
    return fname



def do_worker(q_in, q_out):
    '''
    Do all kinds of calculations for a pair of stocks.
    '''

    while True:
        job = q_in.get()
        if job is None:
            # Mark the end of queue
            q_out.put(None)
            break
        job_id, stock_x, stock_y, df_stock_x, df_stock_y = job
        
        result_df = pd.DataFrame()
        result_df['index'] = [job_id]
        result_df['Stock_1'] = [stock_x]
        result_df['Stock_2'] = [stock_y]
        for metric in METRICS:
            calc_method = getattr(calc, "calc_" + metric)
            metric_res = calc_method(df_stock_x, df_stock_y)
            if type(metric_res) is dict:
                for sub_metric, sub_val in metric_res.items():
                    result_df[metric + '_' + sub_metric] = sub_val
            elif type(metric_res) is list:
                for i, sub_val in enumerate(metric_res):
                    result_df[metric + '_' + i] = sub_val
            else:
                result_df[metric] = metric_res
        done_job = [job_id, stock_x, stock_y, result_df]
        q_out.put(done_job)


def do_io(q_out, num_workers, total_num_jobs):
    '''
    Do the I/O periodically.
    '''
    
    config = load_config()
    output_folder = config['TRAINING_OUTPUT_FOLDER']
    log_file = config['LOG_FILE']

    output_df = pd.DataFrame()
    output_filename = generate_new_output_file(output_folder)

    update_interval = 1000 # File I/O for every this many jobs done
    num_jobs_completed = 0
    start_time = time.time()
    while True:
        job = q_out.get()
        if job is None:
            num_workers -= 1
            if num_workers == 0:
                break
            else:
                continue
        result_df = job[3]
        output_df = pd.concat([output_df, result_df])
        num_jobs_completed += 1
        if num_jobs_completed % update_interval == 0:
            output_df.to_csv(output_filename, index=False)
            cur_time = time.time()
            est_total_time = (total_num_jobs / num_jobs_completed) * (cur_time - start_time)
            est_finish_time = time.strftime("%Y%m%d %H:%M:%S", time.localtime(start_time + est_total_time))
            write_log("%s jobs completed. Est finish time: %s" % (num_jobs_completed, est_finish_time), log_file)
            if num_jobs_completed % (update_interval * 50) == 0:
                output_df = pd.DataFrame()
                output_filename = generate_new_output_file(output_folder)
    output_df.to_csv(output_filename, index=False)
    write_log("All jobs completed. Merging output files...", log_file)
    merge_output(output_folder, os.path.join(output_folder, "out_merged.csv"))
    write_log("All completed. Program Exit", log_file)


def main():
    '''
    Program entry point.
    '''
    
    # 1. Load the config
    config = load_config()
    stock_data_folder = config['STOCK_DATA_FOLDER']
    output_folder = config['TRAINING_OUTPUT_FOLDER']
    log_file = config['LOG_FILE']
    training_start = config['TRAINING_START']
    training_end = config['TRAINING_END']

    write_log("Training program start", log_file)

    # 2. Load and preprocess the stock data
    
    write_log("Loading stock data...", log_file)
    Stock_Data = load_stock_data(stock_data_folder)
    for code, df in Stock_Data.items():
        calc.preprocess(df)
        Stock_Data[code] = df.loc[training_start : training_end]
    write_log("All stock data loaded and preprocessed", log_file)
    
    # 3. Load the job status for the worker
    total_jobs = generate_jobs(stock_data_folder)
    if not os.path.isdir(output_folder):
        os.makedirs(output_folder)

    q_in = Queue()
    q_out = Queue()
    
    write_log("Loading outstanding jobs...", log_file)

    jobs_done_ids = {} # ids of jobs that have been done
    for fname in os.listdir(output_folder):
        fname = os.path.join(output_folder, fname)
        try:
            df = pd.read_csv(fname)
            for job_id in df['index']:
                jobs_done_ids[str(job_id)] = 0
        except Exception as e:
            continue
    
    outstanding_jobs = []
    for job_id, stock_x, stock_y in total_jobs:
        if job_id in jobs_done_ids:
            continue
        stock_x_df, stock_y_df = Stock_Data[stock_x], Stock_Data[stock_y]
        job = [job_id, stock_x, stock_y, stock_x_df, stock_y_df]
        outstanding_jobs.append(job)
    total_num_jobs = len(outstanding_jobs)
    write_log("Outstanding %s jobs" % total_num_jobs, log_file)
    
    # 4. Prepare current output file
    output_filename = generate_new_output_file(output_folder)
    output_df = pd.DataFrame()

    write_log("Training started...", log_file)
        
    # 5A. Spawn child worker processes
    num_workers = os.cpu_count() or 4
    processes = []
    for i in range(num_workers):
        proc_worker = Process(target=do_worker, args=(q_in, q_out))
        processes.append(proc_worker)
    # Another child process to handle File I/O
    proc_io = Process(target=do_io, args=(q_out, num_workers, total_num_jobs))
    processes.append(proc_io)
    # Start all child processes
    for proc in processes:
        proc.start()
    
    # 5B. Parent process will enqueue jobs
    while len(outstanding_jobs) > 0:
        q_in.put(outstanding_jobs.pop(0))
    for i in range(num_workers):
        q_in.put(None) # Mark the end of jobs
    
    # Wait for all child processes to complete
    for proc in processes:
        proc.join()



if __name__ == "__main__":
    main()

