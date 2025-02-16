import pandas as pd
import json
import re
from datetime import datetime
import argparse
import os

def process_log_file(file_path):
    data = []
    
    with open(file_path, 'r') as file:
        for line in file:
            try:
                # Extract query number and job ID
                match = re.search(r'QUERY: (q\d+) JOB_ID: (\w+)', line)
                if match:
                    query_num = match.group(1)
                    job_id = match.group(2)
                    
                    # Extract the JSON part
                    json_str = line[line.find('{'):].strip()
                    job_data = json.loads(json_str)
                    
                    # Extract timestamps
                    timestamps = job_data['timestamps']
                    
                    # Calculate throughput
                    throughput = calculate_throughput(job_data)
                    
                    entry = {
                        'Query_Number': query_num,
                        'Job_ID': job_id,
                        'Total_Duration': job_data['duration'],  # in milliseconds
                        'Parsing_Optimization_Time': timestamps['RUNNING'] - timestamps['CREATED'],
                        'Execution_Time': timestamps['FINISHED'] - timestamps['RUNNING'],
                        'Throughput_Records_Per_Sec': throughput
                    }
                    
                    data.append(entry)
            except Exception as e:
                print(f"Error processing line: {str(e)}")
                continue
    
    return data

def calculate_throughput(job_data):
    try:
        vertex = job_data['vertices'][0]
        metrics = vertex['metrics']
        
        # Get write-records and duration
        records = metrics.get('write-records', 0)
        duration_ms = job_data['duration']
        
        # Calculate records per second (convert duration to seconds)
        if duration_ms > 0:
            return (records * 1000) / duration_ms
        return 0
    except Exception as e:
        print(f"Error calculating throughput: {str(e)}")
        return 0

def main():
    parser = argparse.ArgumentParser(description='Process Nexmark query results log file.')
    parser.add_argument('log_file', help='Path to the log file')
    parser.add_argument('-o', '--output', help='Path to save the Excel file (default: same directory as log file)')
    args = parser.parse_args()

    log_file_path = args.log_file
    if not os.path.exists(log_file_path):
        print(f"Error: File not found: {log_file_path}")
        return

    if args.output:
        output_excel_path = args.output
    else:
        # Use the same directory as the log file, but with .xlsx extension
        output_excel_path = os.path.splitext(log_file_path)[0] + '.xlsx'

    try:
        # Process the log file
        data = process_log_file(log_file_path)
        
        df = pd.DataFrame(data)
        
        df['Total_Duration'] = df['Total_Duration'] / 1000
        df['Parsing_Optimization_Time'] = df['Parsing_Optimization_Time'] / 1000
        df['Execution_Time'] = df['Execution_Time'] / 1000
        
        df['Throughput_Records_Per_Sec'] = df['Throughput_Records_Per_Sec'].round(2)
        
        # Write to Excel
        df.to_excel(output_excel_path, index=False)
        print(f"Excel file created successfully at: {output_excel_path}")

        # Print a summary to console
        print("\nSummary Statistics:")
        print(df.describe())
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
