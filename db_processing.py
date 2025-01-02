import argparse
from procmine import ProcMine

def main(path_data:str,
         path_map:str=None,
         dir_output:str=None,
         file_output:str=None) -> None:
    
    procmine = ProcMine(path_data=path_data, path_map=path_map,
                        dir_output=dir_output, file_output=file_output)
    
    # Load data file, load map file, check output directory, check entities directory
    procmine.prepare_data_paths()

    # Process database
    procmine.process()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TA2 Database Processing')

    parser.add_argument('--path_data', required=True,
                        help='Directory or file where the mineral site database is located')
    
    parser.add_argument('--path_map',
                        help='CSV file with label mapping information.')
    
    parser.add_argument('--output_directory',
                        help='Directory for processed mineral site database')
    
    parser.add_argument('--output_filename', 
                        help='Filename for processed mineral site database')
    
    args = parser.parse_args()

    main(path_data=args.path_data, path_map=args.path_map,
         dir_output=args.output_directory, file_output=args.output_filename)