import argparse
import logging
import os
from datetime import datetime
from trace_collect import proxy_dump_traces
from datetime import datetime, timedelta
from multiprocessing import Pool


def get_normal_data(st_time: datetime, ed_time: datetime, ip: str, port: str, output_dir: str, num_workers: int, num_threads: int, overwrite: bool, limit: int) -> None:
    assert st_time.second == 0

    def get_next_ts_clean_time(t: datetime):
        delta = timedelta(minutes=1)
        while True:
            t += delta
            if t.minute % 10 == 7:
                return t

    tasks = []
    last_time, cur_time = st_time, get_next_ts_clean_time(st_time)
    while cur_time <= ed_time:
        tasks.append({
            'ip': ip,
            'port': port,
            'st_time': last_time,
            'ed_time': cur_time,
            'output_path': os.path.join(output_dir, f'normal_{last_time}_{cur_time}.pkl'),
            'num_threads': num_threads,
            'overwrite': overwrite,
            'limit': limit
        })
        last_time = cur_time + timedelta(minutes=1)
        cur_time += timedelta(minutes=10)

    with Pool(num_workers) as p:
        p.map(proxy_dump_traces, tasks)


""
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--st', dest='st_time', type=str, help='start time', required=True)
    parser.add_argument('-e', '--ed', dest='ed_time', type=str, help='end time', required=True)
    args = parser.parse_args()
    st_time = args.st_time
    ed_time = args.ed_time
    logging.basicConfig(filename='./logs/get_normal_data_run_info.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    time_format = '%Y-%m-%d %H:%M:%S'
    st_time = datetime.strptime(st_time, time_format)
    ed_time = datetime.strptime(ed_time, time_format)
    print(st_time, ed_time)

    get_normal_data(
        st_time=st_time,
        ed_time=ed_time,
        ip='localhost',
        port='13200',
        output_dir='/data1/TraceMDRCA/data/raw_data/normal',
        num_workers=4,
        num_threads=4,
        overwrite=False,
        limit=100000
    )
