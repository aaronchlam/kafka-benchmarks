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

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'rabbitmq', 'increasing-clients'))
PERSISTENCE_DIR = os.path.join(DATA_DIR, 'persistent')

THROUGHPUT_REGEX = r'(\d+) msg/s'
PRODUCER_REGEX = r'^producer-tem\d+(-\d+)*\.txt$'
CONSUMER_REGEX = r'^consumer-\d+\.txt$'


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


if __name__ == '__main__':
    dfs = []
    for num_consumers in range(1, 11):
        throughput_filepath = filepath_to_experiment(DATA_DIR, 1, 1, num_consumers, 20)
        # process_trials(throughput_filepath)
        # process_trial_throughputs(throughput_filepath)
        df = read_throughputs(throughput_filepath)
        df.fillna(0, inplace=True)

        dfs.append(df)

    producer_data = []
    producer_lows = []
    producer_highs = []
    consumer_data = []
    consumer_lows = []
    consumer_highs = []
    for df in dfs:
        low, high = confidence_interval(df, 'throughput_producer')
        producer_lows.append(low)
        producer_highs.append(high)
        producer_data.append(df['throughput_producer'].mean())

        low, high = confidence_interval(df, 'throughput_consumer')
        consumer_lows.append(low)
        consumer_highs.append(high)
        consumer_data.append(df['throughput_consumer'].mean())

    fig, ax = plt.subplots()

    ax.errorbar(range(1, len(dfs) + 1), producer_data, yerr=(producer_lows, producer_highs),
                fmt='-o',
                color='#2980B9',
                barsabove=True,
                ecolor='k',
                capsize=2,
                capthick=2,
                elinewidth=2,
                markersize=8,
                #markeredgewidth=1,
                #markeredgecolor='k',
                label='Producer Throughput')

    ax.errorbar(range(1, len(dfs) + 1), consumer_data, yerr=(consumer_lows, consumer_highs),
                fmt='-^',
                color='#EC7063',
                barsabove=True,
                ecolor='k',
                capsize=2,
                capthick=2,
                elinewidth=2,
                markersize=8,
                #markeredgewidth=1,
                #markeredgecolor='k',
                label='Average Consumer Throughput')

    ax.set_ylim(bottom=0)
    ax.set_xlim(left=0)
    ax.set_title('Throughput vs Number of Consumers')
    ax.set_xlabel('Number of Consumers')
    ax.set_ylabel('Throughput (MB/s)')
    ax.legend()
    fig.savefig('increasing-clients.eps')
    plt.draw()
    plt.show()
