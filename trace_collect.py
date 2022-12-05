import os
import logging
import pickle
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from utils import retry_session
from typing import List, Dict, Tuple


def query_trace_ids(url: str, limit: int, start: int, end: int) -> List[str]:
    session = retry_session(retries=3)
    res = session.get(url, params={
        'limit': limit,
        'start': start,
        'end': end
    })

    trace_ids = []
    try:
        traces = json.loads(res.text)['traces']
        trace_ids = [trace['traceID'] for trace in traces]
    except:
        logging.error(f'Get trace ids error: url: {url}, start: {start}, end: {end}, limit: {limit}')

    if len(trace_ids) >= limit:
        logging.warning(
            f'len trace_ids({len(trace_ids)}) larger than limit({limit}), this may lead to missing trace records, please set larger limit or smaller interval')

    return trace_ids


def query_trace_detail(query_info: Dict) -> Tuple[str, Dict]:
    url = query_info['url']
    trace_id = query_info['trace_id']
    start = query_info['start']
    end = query_info['end']

    session = retry_session(retries=3)
    res = session.get(f'{url}/{trace_id}', params={
        'start': start,
        'end': end
    })
    trace = None
    try:
        trace = json.loads(res.text)
    except:
        logging.error(f'Get trace detail error: url: {url}, trace_id: {trace_id}, start: {start}, end: {end}')
    return trace_id, trace


def dump_traces(ip: str, port: str, st_time: datetime, ed_time: datetime,
                output_path: str, num_threads: int = 4, overwrite: bool = False, limit: int = 100000) -> None:
    if os.path.exists(output_path) and not overwrite:
        logging.info(f'Skip exists file {output_path}')
        return

    query_trace_ids_url = f'http://{ip}:{port}/api/search'
    start = int(st_time.timestamp())
    end = int(ed_time.timestamp())
    trace_ids = query_trace_ids(query_trace_ids_url, limit, start, end)

    query_trace_detail_url = f'http://{ip}:{port}/api/traces'
    query_tasks = [{
        'url': query_trace_detail_url,
        'trace_id': trace_id,
        'start': start,
        'end': end
    } for trace_id in trace_ids]
    data = {}
    executor = ThreadPoolExecutor(max_workers=num_threads)
    for trace_id, trace in executor.map(query_trace_detail, query_tasks):
        data[trace_id] = trace

    with open(output_path, 'wb') as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

    logging.info(f'Saved {output_path}')


def proxy_dump_traces(info: Dict) -> None:
    ip = info['ip']
    port = info['port']
    st_time = info['st_time']
    ed_time = info['ed_time']
    output_path = info['output_path']
    num_threads = info['num_threads']
    overwrite = info['overwrite']
    limit = info['limit']

    dump_traces(ip, port, st_time, ed_time, output_path, num_threads, overwrite, limit)
