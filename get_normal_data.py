import argparse
import logging
import os
from datetime import datetime
from trace_collect import proxy_dump_traces
from datetime import datetime, timedelta
from multiprocessing import Pool


def get_normal_data(st_time: datetime, ed_time: datetime, ip: str, port: str, output_dir: str, num_workers: int, num_threads: int, overwrite: bool, limit: int) -> None:
    assert st_time.second == 0

    normal_st_seconds = [0, 15, 30, 45]
    normal_delta = 9

    def get_next_ts_clean_time(t: datetime):
        next_t = 60
        for st_second in normal_st_seconds:
            if st_second >= t.minute:
                next_t = st_second
                break
        return t + timedelta(minutes=next_t - t.minute)

    tasks = []
    st = get_next_ts_clean_time(st_time)
    ed = min(st + timedelta(minutes=normal_delta), ed_time)
    print('tasks:')
    while st < ed_time:
        print(st.strftime('%Y-%m-%dT%H:%M:%S'), '->', ed.strftime('%Y-%m-%dT%H:%M:%S'))
        tasks.append({
            'ip': ip,
            'port': port,
            'st_time': st,
            'ed_time': ed,
            'output_path': os.path.join(output_dir, f"normal_{st.strftime('%Y-%m-%dT%H:%M:%S')}_{ed.strftime('%Y-%m-%dT%H:%M:%S')}.pkl"),
            'num_threads': num_threads,
            'overwrite': overwrite,
            'limit': limit
        })
        st = get_next_ts_clean_time(ed)
        ed = min(st + timedelta(minutes=normal_delta), ed_time)

    # with Pool(num_workers) as p:
    #     p.map(proxy_dump_traces, tasks)


""
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--st', dest='st_time', type=str, help='start time', required=True)
    parser.add_argument('-e', '--ed', dest='ed_time', type=str, help='end time', required=True)
    args = parser.parse_args()
    st_time = args.st_time
    ed_time = args.ed_time
    logging.basicConfig(filename='./logs/get_normal_data_run_info.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    time_format = '%Y-%m-%dT%H:%M:%S'
    st_time = datetime.strptime(st_time, time_format)
    ed_time = datetime.strptime(ed_time, time_format)
    print('st:', st_time.strftime(time_format), 'ed:', ed_time.strftime(time_format))

    get_normal_data(
        st_time=st_time,
        ed_time=ed_time,
        ip='localhost',
        port='13200',
        output_dir='/data1/TraceMDRCA/data/raw_data/normal',
        num_workers=4,
        num_threads=2,
        overwrite=False,
        limit=100000
    )
