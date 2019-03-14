
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



def generate_jobs(stock_data_folder, filename):
    '''
    Generate all the jobs.
    Each job has an id and two stock codes to represent a pair.
    '''
    
    stock_codes = [get_stock_code(fname) for fname in os.listdir(stock_data_folder)]
    stock_codes.sort()
    num_stocks = len(stock_codes)
    with open(filename, 'w') as F:
        job_index = 0
        i = 0
        while i < num_stocks:
            j = i + 1
            while j < num_stocks:
                F.write("%d\t%s\t%s\n" % (job_index, stock_codes[i], stock_codes[j]))
                j += 1
                job_index += 1
            i += 1


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
    Calculate all kinds of correlation for a pair of stocks.
    '''

    while True:
        job = q_in.get()
        if job is None:
            # Mark the end of queue
            q_out.put(None)
            break
        job_id, stock_x, stock_y, df_stock_x, df_stock_y = job
        
        result_df = pd.DataFrame(columns=(['index', 'Stock_1', 'Stock_2'] + calc.metrics))
        result_df['index'] = [job_id]
        result_df['Stock_1'] = [stock_x]
        result_df['Stock_2'] = [stock_y]
        for metric in calc.metrics:
            metric_calc_method = getattr(calc, "calc_" + metric)
            result_df[metric] = metric_calc_method(df_stock_x, df_stock_y)

        done_job = [job_id, stock_x, stock_y, result_df]
        q_out.put(done_job)



def main():
    '''
    Program entry point.
    '''
    
    # 1. Load the config
    config = load_config()
    stock_data_folder = config['STOCK_DATA_FOLDER']
    output_folder = config['TRAINING_OUTPUT_FOLDER']
    job_in_file = config['TRAINING_JOB_IN_FILE']
    log_file = config['LOG_FILE']
    training_start = config['TRAINING_START']
    training_end = config['TRAINING_END']

    write_log("Training program start", log_file)

    # 2. Load and preprocess the stock data
    
    write_log("Loading stock data...", log_file)
    Stock_Data = load_stock_data(stock_data_folder)
    for code, df in Stock_Data.items():
        calc.preprocess(df)
        Stock_Data[code] = df[(df['Date'] >= training_start) & (df['Date'] <= training_end)]
    write_log("All stock data loaded and preprocessed", log_file)
    
    # 3. Load the job status for the worker
    if not os.path.isfile(job_in_file):
        # If no job status file is found, assign jobs
        create_dir_and_file(job_in_file)
        write_log("Generating jobs...", log_file)
        generate_jobs(stock_data_folder, job_in_file)
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
        except Exception as e:
            write_log("When loading output '%s', error: %s" % (fname, str(e)), log_file)
            continue
        for job_id in df['index']:
            jobs_done_ids[str(job_id)] = 0
    
    total_num_jobs = 0
    with open(job_in_file) as F:
        for line in F:
            job_id, stock_x, stock_y = line.strip().split('\t')[:3]
            if job_id in jobs_done_ids:
                continue
            if stock_x not in Stock_Data:
                write_log("Missing stock data for %s" % stock_x, log_file)
                continue
            if stock_y not in Stock_Data:
                write_log("Missing stock data for %s" % stock_y, log_file)
                continue
            stock_x_df, stock_y_df = Stock_Data[stock_x], Stock_Data[stock_y]
            job = [job_id, stock_x, stock_y, stock_x_df, stock_y_df]
            q_in.put(job)
            total_num_jobs += 1
    write_log("Loaded %s jobs" % total_num_jobs, log_file)
    
    # 4. Prepare current output file
    output_filename = generate_new_output_file(output_folder)
    output_df = pd.DataFrame()

    write_log("Training started...", log_file)
        
    # 5A. Spawn child processes and let them start to do jobs
    num_processes = os.cpu_count() or 4
    for i in range(num_processes):
        q_in.put(None) # Mark the end of input queue
    for i in range(num_processes):
        proc = Process(target=do_worker, args=(q_in, q_out))
        proc.start()

    # 5B. Parent process will listen to the output queue and handle file I/O periodically
    num_jobs_completed = 0
    update_interval = 1000 # File I/O for every this many jobs done
    start_time = time.time()
    while True:
        job = q_out.get()
        if job is None:
            num_processes -= 1
            if num_processes == 0:
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
            write_log("%s out of %s jobs completed. Est finish time: %s" % (num_jobs_completed, total_num_jobs, est_finish_time), log_file)
            if num_jobs_completed % (update_interval * 50) == 0:
                output_df = pd.DataFrame()
                output_filename = generate_new_output_file(output_folder)
    output_df.to_csv(output_filename, index=False)

    write_log("All jobs completed. Training program exit", log_file)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(str(e))
        input()

