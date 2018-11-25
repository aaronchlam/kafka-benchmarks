import os
import csv
import re
from datetime import datetime

import pandas
import matplotlib.pyplot as plt

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))

THROUGHPUT_REGEX = r'\((\d+\.\d+) \w{2}\/sec\)'
CONSUMER_REGEX = r'^consumer-tem\d+(-\d+)*\.txt$'


def skip_last(iterator):
    prev = next(iterator)
    for item in iterator:
        yield prev
        prev = item


def read_producer_throughput(filepath):
    rows = []
    with open(filepath, 'r') as file:
        reader = csv.reader(file, skipinitialspace=True, delimiter=',')
        for row in skip_last(reader):
            dt = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')

            match = re.search(THROUGHPUT_REGEX, row[2])
            throughput = match.group(1)

            # new_row = [dt, float(throughput), row[1], row[-2], row[-1]]
            rows.append([dt, float(throughput)])
    #return pandas.DataFrame(rows, columns=["timestamp", "throughput", "records_sent", "avg_latency", "max_latency"])
    return pandas.DataFrame(rows, columns=['timestamp', 'throughput'])


def read_consumer_throughput(filepath):
    rows = []
    with open(filepath, 'r') as file:
        reader = csv.reader(file, skipinitialspace=True, delimiter=',')
        next(reader)
        for row in skip_last(reader):
            dt = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S:%f').replace(microsecond=0)

            rows.append([dt, float(row[3])])
    return pandas.DataFrame(rows, columns=['timestamp', 'throughput'])


def discard_beginning(df, time=30):
    threshold_time = df['timestamp'].iloc[0] + pandas.Timedelta(seconds=time)
    return df.loc[(df['timestamp'] > threshold_time)]


def get_window(df, time=300):
    threshold_time = df['timestamp'].iloc[0] + pandas.Timedelta(seconds=time)
    return df.loc[df['timestamp'] <= threshold_time]


def read_producer_trials(data_dir):
    producer_dfs = {}
    for root, dir, files in os.walk(data_dir):
        for file in files:
            if file == 'producer.txt':
                filepath = os.path.join(root, file)
                trial_dir = os.path.basename(root)
                producer_dfs[trial_dir] = get_window(discard_beginning(read_producer_throughput(filepath)))
    return producer_dfs


def read_consumer_trials(data_dir):
    consumer_dfs = {}
    for root, dir, files in os.walk(data_dir):
        for file in files:
            if re.match(CONSUMER_REGEX, file):
                filepath = os.path.join(root, file)
                trial_dir = os.path.basename(root)
                consumer_dfs[trial_dir] = get_window(discard_beginning(read_consumer_throughput(filepath)))
    return consumer_dfs


def filepath_to_experiment(num_replicas, num_producers, num_consumers, producer_throughput):
    return os.path.join(DATA_DIR, '{}-replicas'.format(num_replicas), '{}-producers'.format(num_producers),
                        '{}-consumers'.format(num_consumers), '{}-throughput'.format(producer_throughput))


if __name__ == '__main__':
    print('data_dir: {}'.format(DATA_DIR))
    producer_filepath = os.path.join(DATA_DIR, '1-replicas', '1-producers', '1-consumers', '5-throughput', '0-trial',
                                 'producer.txt')
    consumer_filepath = os.path.join(DATA_DIR, '1-replicas', '1-producers', '1-consumers', '5-throughput', '0-trial',
                                     'consumer-tem10.txt')

    big_producer_dfs = {}
    big_consumer_dfs = {}
    for throughput in range(5, 60, 5):
        producer_dfs = read_producer_trials(filepath_to_experiment(1, 1, 1, throughput))
        consumer_dfs = read_consumer_trials(filepath_to_experiment(1, 1, 1, throughput))

        big_producer_df = pandas.concat(producer_dfs.values())
        big_consumer_df = pandas.concat(consumer_dfs.values())

        big_producer_dfs[throughput] = big_producer_df
        big_consumer_dfs[throughput] = big_consumer_df

    producer_means = [df['throughput'].mean() for df in big_producer_dfs.values()]
    consumer_means = [df['throughput'].mean() for df in big_consumer_dfs.values()]

    plt.plot(producer_means, consumer_means, 'ro')
    plt.ylabel('Consumer Throughput (MB/s)')
    plt.xlabel('Producer Throughput(MB/s)')
    plt.show()