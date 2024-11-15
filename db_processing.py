import time
from datetime import datetime

import logging
import argparse

from utils.data import *

def main(path_rawdata:str, path_attribute_map:str|None, path_output_directory:str, filename_output:str):
    # Terminate program if output file name doesn't exist
    if not filename_output:
        return -1
    
    # Initiate logger
    check_directory_path('./logs')
    logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO, 
                        handlers=[
                            logging.FileHandler(f'./logs/process_data_to_schema_{datetime.timestamp(datetime.now())}.log'),
                            logging.StreamHandler()
                        ])
    
    if not path_attribute_map:
        logging.info(f'Attribute map cannot be found. Using column identification tool. This may lead to incorrect processing results.')
    
    # Create output directory path if it doesn't exist
    check_directory_path(path_output_directory)

    print('Init')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TA2 Database Processing')
    parser.add_argument('--raw_data', required=True,
                        help='Directory or file where the mineral site database is located')
    parser.add_argument('--attribute_map',
                        help='CSV file with label mapping information.')
    parser.add_argument('--output_directory', default='./outputs/',
                        help='Directory for processed mineral site database')
    parser.add_argument('--output_filename', required=True, 
                        help='Filename for processed mineral site database')
    args = parser.parse_args()

    main(path_rawdata=args.raw_data,
         path_attribute_map=args.attribute_map,
         path_output_directory=args.output_directory,
         filename_output=args.output_filename)
