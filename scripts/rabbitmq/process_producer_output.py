import os
import csv
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import bitmath

import pandas
import matplotlib.pyplot as plt
import scipy
import scikits.bootstrap as bootstrap

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'rabbitmq'))
PERSISTENCE_DIR = os.path.join(DATA_DIR, 'persistent')

THROUGHPUT_REGEX = r'(\d+) msg/s'
PRODUCER_REGEX = r'^producer-tem\d+(-\d+)*\.txt$'
CONSUMER_REGEX = r'^consumer-tem\d+(-\d+)*\.txt$'


def filepath_to_experiment(data_dir, num_replicas, num_producers, num_consumers, producer_throughput):
    return os.path.join(data_dir, '{}-nodes'.format(num_replicas), '{}-producers'.format(num_producers),
                        '{}-consumers'.format(num_consumers), '{}-throughput'.format(producer_throughput))


def process_trials(data_dir):
    for root, dir, files in os.walk(data_dir):
        if len(files) in (0, 1):
            continue

        producer_df = None
        consumer_df = None
        for file in files:
            filepath = os.path.join(root, file)

            if re.match(PRODUCER_REGEX, file):
                producer_df = read_throughput(filepath)
            elif re.match(CONSUMER_REGEX, file):
                consumer_df = read_throughput(filepath)

        merged = pandas.DataFrame.merge(producer_df, consumer_df, how='outer', on='timestamp',
                                        suffixes=['_producer', '_consumer'], sort=True)

        merged.to_csv(os.path.join(root, 'throughputs.csv'), index=False)


def read_throughput(filepath):
    rows = []
    with open(filepath, 'r') as file:
        lines = file.readlines()[2:-2]
        for line in lines:
            row = [s.strip() for s in line.split(',')]

            dt = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')

            match = re.search(THROUGHPUT_REGEX, row[3])
            msgs_per_sec = int(match.group(1))

            throughput = bitmath.Byte(msgs_per_sec * 512).to_MB().value

            rows.append([dt, msgs_per_sec, throughput])

    total = 0
    start_idx = 0
    for i, row in enumerate(rows):
        if total > (1e7 / 4):
            start_idx = i
            break
        total += row[1]

    return pandas.DataFrame(rows[start_idx:], columns=['timestamp', 'msgs_per_sec', 'throughput'])


def process_trial_throughputs(data_dir):
    dfs = []

    for root, dir, files in os.walk(data_dir):
        if dir:
            continue

        for file in files:
            if 'throughputs.csv' == file:
                filepath = os.path.join(root, file)
                df = pandas.read_csv(filepath)
                dfs.append(df)

    concat = pandas.concat(dfs)

    out_path = os.path.join(data_dir, 'throughputs.csv')

    concat.to_csv(out_path, index=False)

    return concat

def confidence_interval(df, column):
    # confidence intervals
    low, high = bootstrap.ci(df[column], statfunction=scipy.mean)
    return df[column].mean() - low, high - df[column].mean()


def read_throughputs(data_dir):
    for root, dir, files in os.walk(data_dir):
        if dir:
            for file in files:
                if file == 'throughputs.csv':
                    return pandas.read_csv(os.path.join(root, file))


def unpack_throughput_data(throughput_data, replicas):
    means = sorted(throughput_data[replicas], key=lambda t: t[0])
    return zip(*means)


def get_persistent_data(data_dir):
    throughput_data = defaultdict(list)
    for replicas in range(2, 4):
        for throughput in range(1, 10):
            throughput_filepath = filepath_to_experiment(data_dir, replicas, 1, 1, throughput)
            # process_trials(throughput_filepath)
            # process_trial_throughputs(throughput_filepath)
            df = read_throughputs(throughput_filepath)
            df.fillna(0)

            dfs[replicas][throughput] = df

            low, high = confidence_interval(df, 'throughput_consumer')

            throughput_data[replicas].append((df['throughput_producer'].mean(), df['throughput_consumer'].mean(),
                                              low, high))

    return throughput_data


if __name__ == '__main__':
    dfs = defaultdict(dict)

    throughput_data = defaultdict(list)
    for replicas in range(2, 4):
        for throughput in range(1, 15):
            throughput_filepath = filepath_to_experiment(DATA_DIR, replicas, 1, 1, throughput)
            # process_trials(throughput_filepath)
            # process_trial_throughputs(throughput_filepath)
            df = read_throughputs(throughput_filepath)
            df.fillna(0)

            dfs[replicas][throughput] = df

            low, high = confidence_interval(df, 'throughput_consumer')

            throughput_data[replicas].append((df['throughput_producer'].mean(), df['throughput_consumer'].mean(),
                                              low, high))

    persistent_data = get_persistent_data(PERSISTENCE_DIR)

    fig, ax = plt.subplots()

    for replicas in range(2, 4):
        means = sorted(throughput_data[replicas], key=lambda t: t[0])
        producers, consumers, low, high = zip(*means)

    # producers, consumers, low, high = unpack_throughput_data(1)
    # ax.errorbar(producers, consumers, yerr=(low, high),
    #             fmt='-o',
    #             color='#5DADE2',
    #             barsabove=True,
    #             ecolor='k',
    #             capsize=2,
    #             capthick=2,
    #             elinewidth=2,
    #             markersize=8,
    #             #markeredgewidth=1,
    #             #markeredgecolor='k',
    #             label='replication-factor=1, persistent=False')

    producers, consumers, low, high = unpack_throughput_data(throughput_data, 2)
    ax.errorbar(producers[:7] + producers[-1:], consumers[:7] + consumers[-1:], yerr=(low[:7] + low[-1:], high[:7] + high[-1:]),
                 fmt='-^',
                 barsabove=True,
                 color='#58D68D',
                 ecolor='k',
                 capsize=2,
                 capthick=2,
                 elinewidth=2,
                 markersize=8,
                 #markeredgewidth=1,
                 #markeredgecolor='k',
                 label='persistent=False')

    producers, consumers, low, high = unpack_throughput_data(persistent_data, 2)
    ax.errorbar(producers[:3] + producers[-1:], consumers[:3] + consumers[-1:], yerr=(low[:3] + low[-1:], high[:3] + high[-1:]),
                 fmt='-o',
                 barsabove=True,
                 color='#633974',
                 ecolor='k',
                 capsize=2,
                 capthick=2,
                 elinewidth=2,
                 markersize=8,
                 #markeredgewidth=1,
                 #markeredgecolor='k',
                 label='persistent=True')

    ax.set_ylim(bottom=0)
    ax.set_xlim(left=0)
    ax.set_title('Replication Factor of 2')
    ax.set_xlabel('Producer Throughput (MB/s)')
    ax.set_ylabel('Consumer Throughput (MB/s)')
    ax.legend()
    fig.savefig('replicas-2.eps')

    fig, ax = plt.subplots()

    producers, consumers, low, high = unpack_throughput_data(throughput_data, 3)
    ax.errorbar(producers[:7] + producers[-1:], consumers[:7] + consumers[-1:], yerr=(low[:7] + low[-1:], high[:7] + high[-1:]),
                 fmt='-s',
                 color='#EB984E',
                 barsabove=True,
                 ecolor='k',
                 capsize=2,
                 capthick=2,
                 elinewidth=2,
                 markersize=8,
                 # markeredgewidth=1,
                 # markeredgecolor='k',
                 label='persistent=False')

    producers, consumers, low, high = unpack_throughput_data(persistent_data, 3)
    ax.errorbar(producers[:3] + producers[-1:], consumers[:3] + consumers[-1:], yerr=(low[:3] + low[-1:], high[:3] + high[-1:]),
                 fmt='-s',
                 color='#1F618D',
                 barsabove=True,
                 ecolor='k',
                 capsize=2,
                 capthick=2,
                 elinewidth=2,
                 markersize=8,
                 # markeredgewidth=1,
                 # markeredgecolor='k',
                 label='persistent=True')

    ax.set_ylim(bottom=0)
    ax.set_xlim(left=0)
    ax.set_title('Replication Factor of 3')
    ax.set_xlabel('Producer Throughput (MB/s)')
    ax.set_ylabel('Consumer Throughput (MB/s)')
    ax.legend()
    fig.savefig('replicas-3.eps')
