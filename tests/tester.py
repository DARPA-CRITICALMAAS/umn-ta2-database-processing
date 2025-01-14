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

    # Save output
    procmine.save_output()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TA2 Database Processing')

    parser.add_argument('--csv', action='store_true',
                        help='Test loading csv. Assume csv path at ')
    
    parser.add_argument('--xlsx', action='store_true',
                        help='Test loading csv. Assume csv path at ')
    
    parser.add_argument('--gdb', action='store_true',
                        help='Test loading csv. Assume csv path at ')
    
    parser.add_argument('--json', action='store_true',
                        help='Test loading csv. Assume csv path at ')
    
    parser.add_argument('--dir', action='store_true',
                        help='Test loading csv. Assume csv path at ')
    
    args = parser.parse_args()

    main(path_data=args.path_data, path_map=args.path_map,
         dir_output=args.output_directory, file_output=args.output_filename)