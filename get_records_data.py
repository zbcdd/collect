import argparse
import logging
import os
import pandas as pd
from datetime import datetime
from trace_collect import proxy_dump_traces
from datetime import datetime
from multiprocessing import Pool


def get_records_data(df: pd.DataFrame, ip: str, port: str, output_dir: str, num_workers: int, num_threads: int, overwrite: bool, limit: int) -> None:
    """Get trace data from records file

    Args:
        df (pd.DataFrame): Records df
        ip (str): Tempo api ip
        port (str): Tempo api port
        output_dir (str): output dir
        num_workers (int): the number of records processed parallel
        num_threads (int): the number of threads when process one record
        overwrite (bool): set True to overwrite file if the file exists
        limit (int): trace records limit count
    """
    tasks = []
    for _, row in df.iterrows():
        st_time = datetime.strptime(row['st_time'].split('.')[0], '%Y-%m-%dT%H:%M:%S')
        ed_time = datetime.strptime(row['ed_time'].split('.')[0], '%Y-%m-%dT%H:%M:%S')
        output_path = os.path.join(output_dir, f"{row['filename']}.pkl")
        tasks.append({
            'ip': ip,
            'port': port,
            'st_time': st_time,
            'ed_time': ed_time,
            'output_path': output_path,
            'num_threads': num_threads,
            'overwrite': overwrite,
            'limit': limit
        })

    with Pool(num_workers) as p:
        p.map(proxy_dump_traces, tasks)


"sudo python3 get_records_data.py --records ./records/2023-01-15.csv"
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--records', dest='records', type=str, help='records path', required=True)
    args = parser.parse_args()
    record_path = args.records
    logging.basicConfig(filename='./logs/get_records_data_run_info.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    get_records_data(
        df=pd.read_csv(record_path),
        ip='localhost',
        port='13200',
        output_dir='/data1/TraceMDRCA/data/raw_data/fault',
        num_workers=4,
        num_threads=4,
        overwrite=False,
        limit=100000
    )
