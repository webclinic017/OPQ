
import os
import sys

import pandas as pd

from util import *

def main(argc, argv):
    if argc < 3:
        print("Usage: merge_output.py <folder_of_files> <target_file>")
        return 1
    input_folder = argv[1]
    output_file = argv[2]

    create_dir_and_file(output_file)

    out_df = pd.DataFrame()
    for fname in os.listdir(input_folder):
        fname = os.path.join(input_folder, fname)
        print("Merging %s" % fname)
        df = pd.read_csv()
        out_df = pd.concat([out_df, df])
            
    out_df = out_df.drop_duplicates(subset='index', keep='first')
    
    out_df.to_csv(output_file, index=False)
    print("Merged complete")

    return 0


if __name__ == "__main__":
    main(len(sys.argv), sys.argv)
