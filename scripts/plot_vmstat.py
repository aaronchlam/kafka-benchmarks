import os
import csv
import re
from datetime import datetime

import pandas
from matplotlib import pyplot as plt
import numpy
import scipy
import scikits.bootstrap as bootstrap

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

            rows.append([dt, float(throughput)])
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


def read_vmstat(filepath):
    rows = []
    with open(filepath, 'r') as file:
        reader = csv.reader(file, skipinitialspace=True, delimiter=' ')
        column_names = None
        for line in reader:
            if reader.line_num == 1:
                continue
            elif reader.line_num == 2:
                column_names = line
                column_names[-1] = 'timestamp'
            else:
                row = []
                row.append(int(line[9]))
                row.append(datetime.strptime(' '.join(line[-2:]), '%Y-%m-%d %H:%M:%S'))
                rows.append(row)
        return pandas.DataFrame(rows, columns=['bo', 'timestamp'])


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


def convert_timestamps_to_seconds_elapsed(df):
    first_timestamp = df['timestamp'][0]
    df['seconds'] = (df['timestamp'] - first_timestamp).dt.total_seconds()


if __name__ == '__main__':
    trial_filepath = os.path.join(DATA_DIR, '1-replicas', '1-producers', '1-consumers', '15-throughput', '4-trial')
    vmstat_filepath = os.path.join(trial_filepath, 'vmstat-tem08.txt')
    vmstat_df = read_vmstat(vmstat_filepath)
    vmstat_df['throughput'] = vmstat_df['bo'].apply(lambda x: x * 1024 / 1e6)
    convert_timestamps_to_seconds_elapsed(vmstat_df)

    producer_df = read_producer_throughput(os.path.join(trial_filepath, 'producer.txt'))
    convert_timestamps_to_seconds_elapsed(producer_df)

    joined = pandas.merge(vmstat_df, producer_df, how='left', on='timestamp', suffixes=['_vmstat', '_producer'])
    joined.fillna(0, inplace=True)
    joined['seconds_vmstat'] -= 8

    consumer_df = read_consumer_throughput(os.path.join(trial_filepath, 'consumer-tem10.txt'))
    convert_timestamps_to_seconds_elapsed(consumer_df)

    joined_consumer_df = pandas.merge(vmstat_df, consumer_df, how='left', on='timestamp', suffixes=['_vmstat', '_consumer'])
    joined_consumer_df.fillna(0, inplace=True)
    joined_consumer_df['seconds_vmstat'] -= 11

    with pandas.option_context('display.max_rows', None, 'display.max_columns', None):
        sums = []
        for i in range(0, 390, 30):
            sums.append(vmstat_df.loc[i:i + 30].sum()['throughput'])
        print(numpy.mean(sums))
        print(bootstrap.ci(sums, statfunction=scipy.mean))

    fig, axes = plt.subplots(nrows=2, ncols=1)

    joined.loc[(joined['seconds_vmstat'] >= 0)] \
        .plot(x='seconds_vmstat', y='throughput_vmstat', c='#58D68D', ax=axes[0])

    joined_consumer_df.loc[(joined_consumer_df['seconds_consumer'] != 0) | (joined['seconds_vmstat'] < 1)] \
        .loc[(joined['seconds_vmstat'] >= 0)] \
        .plot(x='seconds_vmstat', y='throughput_consumer', c='#5DADE2', label='Consumer', ax=axes[1])

    # disk
    joined.loc[(joined['seconds_producer'] != 0) | (joined['seconds_vmstat'] > 355)] \
        .loc[(joined['seconds_vmstat'] >= 0)] \
        .plot(x='seconds_vmstat', y='throughput_producer', c='#EB984E', label='Producer', ax=axes[1])

    #joined_consumer_df.plot(x='seconds_vmstat', y='throughput_consumer', ax=ax, label='Consumer')
    # plt.fill_between(vmstat_df['timestamp'],
    #              fmt='-o',
    #              barsabove=True,
    #              ecolor='k',
    #              capsize=2,
    #              capthick=2,
    #              elinewidth=2,
    #              label='1 Replica')

    #print(vmstat_df.mean())

    #plt.xticks(numpy.arange(0, x_end + 1, 60))
    #y_start, y_end = axes[0].get_ylim()
    axes[1].set_title('Producer & Consumer Throughputs Over Time')
    axes[0].set_title('Broker Disk Write Throughput Over Time')
    for ax in axes:
        x_end = 360
        ax.set_xlim(0, x_end - ((x_end + 60) % (60)))
        ax.set_xticks(numpy.arange(0, x_end + 1, 60))
        ax.set_ylim(bottom=0)
        ax.set_ylabel('Throughput (MB/s)')
        ax.set_xlabel('Time (s)')
    #plt.yticks(numpy.arange(0, y_end + 1, 20))
    axes[0].get_legend().remove()
    plt.tight_layout()
    # plt.show()
    plt.draw()
    plt.savefig('./throughput_vmstat.eps')