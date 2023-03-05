import json
import pandas as pd
from datetime import datetime
from utils import retry_session



def count(min_duration, start, end, limit=100000):
    session = retry_session(3)
    res = json.loads(session.get('http://localhost:13200/api/search',
                      params={
                        'limit': limit,
                        'start': start,
                        'end': end,
                        'minDuration': min_duration
                      }).text)
    if 'traces' in res:
        return len(res['traces'])
    return 0


if __name__ == '__main__':
    record = './records/2023-03-05T13:45:00_2023-03-05T22:40:00.csv'
    record_df = pd.read_csv(record)
    date_format = '%Y-%m-%dT%H:%M:%S'
    for st_time, ed_time in zip(record_df['st_time'], record_df['ed_time']):
        start, end = int(datetime.timestamp(datetime.strptime(st_time, date_format))), int(datetime.timestamp(datetime.strptime(ed_time, date_format)))
        cnt = count('2s', start, end)
        postfix = ' !!!!!!!!!' if cnt > 10 else ''
        print(start, cnt, postfix)
