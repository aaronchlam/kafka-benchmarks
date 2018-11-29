import os
import csv
import re
from datetime import datetime
from pathlib import Path

import pandas
import matplotlib.pyplot as plt
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


def compute_throughput_ci(dataframe):
    low, high = bootstrap.ci(dataframe['throughput'], statfunction=scipy.mean)
    return low, high


def get_2_replica_dfs(data_dir):
    producer_dfs = []
    consumer_dfs = []
    for throughput in range(5, 60, 5):
        producer_trial_dfs = read_producer_trials(filepath_to_experiment(2, 1, 1, throughput))
        consumer_trial_dfs = read_consumer_trials(filepath_to_experiment(2, 1, 1, throughput))

        concat_producer_dfs = pandas.concat(producer_trial_dfs.values())
        concat_consumer_dfs = pandas.concat(consumer_trial_dfs.values())

        producer_dfs.append(concat_producer_dfs)
        consumer_dfs.append(concat_consumer_dfs)

    return sorted(zip(producer_dfs, consumer_dfs), key=lambda t: t[0]['throughput'].mean())


def get_3_replica_dfs(data_dir):
    producer_dfs = []
    consumer_dfs = []
    for throughput in range(5, 60, 5):
        producer_trial_dfs = read_producer_trials(filepath_to_experiment(3, 1, 1, throughput))
        consumer_trial_dfs = read_consumer_trials(filepath_to_experiment(3, 1, 1, throughput))

        concat_producer_dfs = pandas.concat(producer_trial_dfs.values())
        concat_consumer_dfs = pandas.concat(consumer_trial_dfs.values())

        producer_dfs.append(concat_producer_dfs)
        consumer_dfs.append(concat_consumer_dfs)

    return sorted(zip(producer_dfs, consumer_dfs), key=lambda t: t[0]['throughput'].mean())


def confidence_interval(dfs):
    # confidence intervals
    lows = []
    highs = []
    for df in dfs:
        low, high = compute_throughput_ci(df)
        lows.append(df['throughput'].mean() - low)
        highs.append(high - df['throughput'].mean())

    return lows, highs


if __name__ == '__main__':
    big_producer_dfs = []
    big_consumer_dfs = []
    for throughput in range(5, 60, 5):
        producer_dfs = read_producer_trials(filepath_to_experiment(1, 1, 1, throughput))
        consumer_dfs = read_consumer_trials(filepath_to_experiment(1, 1, 1, throughput))

        big_producer_df = pandas.concat(producer_dfs.values())
        big_consumer_df = pandas.concat(consumer_dfs.values())

        big_producer_dfs.append(big_producer_df)
        big_consumer_dfs.append(big_consumer_df)

    producer_means = [df['throughput'].mean() for df in big_producer_dfs]
    consumer_means = [df['throughput'].mean() for df in big_consumer_dfs]

    # confidence intervals
    lows = []
    highs = []
    for df in big_consumer_dfs:
        low, high = compute_throughput_ci(df)
        lows.append(df['throughput'].mean() - low)
        highs.append(high - df['throughput'].mean())
        #lows.append(df['throughput'].mean() - df.quantile(0.05))
        #highs.append(df.quantile(0.95) - df['throughput'].mean())


    # 2-replicas
    replicas_2_dfs_tuples = get_2_replica_dfs(DATA_DIR)
    replicas_2_lows, replicas_2_highs = confidence_interval(map(lambda t: t[1], replicas_2_dfs_tuples))

    # 3-replicas
    replicas_3_dfs_tuples = get_3_replica_dfs(DATA_DIR)
    replicas_3_lows, replicas_3_hights = confidence_interval(map(lambda t: t[1], replicas_3_dfs_tuples))

    print(producer_means[-1].mean())
    print(replicas_2_dfs_tuples[-1][0].mean())
    print(replicas_3_dfs_tuples[-1][0].mean())

    #plt.plot(producer_means, consumer_means, 'ro', label='1 Replica')
    plt.errorbar(producer_means, consumer_means, yerr=[lows, highs],
                 fmt='-o',
                 color='#5DADE2',
                 barsabove=True,
                 ecolor='k',
                 capsize=2,
                 capthick=2,
                 elinewidth=2,
                 markersize=8,
                 #markeredgewidth=1,
                 #markeredgecolor='k',
                 label='replication-factor=1')
    plt.errorbar([df['throughput'].mean() for df in map(lambda t: t[0], replicas_2_dfs_tuples)],
                 [df['throughput'].mean() for df in map(lambda t: t[1], replicas_2_dfs_tuples)],
                 yerr=[replicas_2_lows, replicas_2_highs],
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
                 label='replication-factor=2')
    plt.errorbar([df['throughput'].mean() for df in map(lambda t: t[0], replicas_3_dfs_tuples)],
                 [df['throughput'].mean() for df in map(lambda t: t[1], replicas_3_dfs_tuples)],
                 yerr=[replicas_3_lows, replicas_3_hights],
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
                 label='replication-factor=3')
    plt.ylim(bottom=0)
    plt.xlim(left=0)
    plt.ylabel('Consumer Throughput (MB/s)')
    plt.xlabel('Producer Throughput (MB/s)')
    plt.title('producers=1, consumers=1, topics=1, partitions=1, record-size=512 bytes')
    plt.legend()
    plt.draw()
    plt.savefig('./producer_throughput.eps', bbox_inches='tight')