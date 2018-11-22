import os

import paramiko

DATA_DIR = '/home/achlam/kafka-benchmarks/data'

USERNAME = 'achlam'

BROKERS_FILE = 'brokers'
CONSUMERS_FILE = 'consumers'
PRODUCERS_FILE = 'producers'


def get_hostnames(filename):
    hostnames = []
    with open(filename, 'r') as f:
        for hostname in f:
            hostnames.append(hostname.lower().strip())
    return hostnames


def open_ssh(hostname):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, username=USERNAME)
    return client


def mkdir_benchmark_results(num_replicas, num_producers, num_consumers, producer_throughput):
    mkdir_path = os.path.join(DATA_DIR, '{}-replicas'.format(num_replicas),
                              '{}-producers'.format(num_producers), '{}-consumers'.format(num_consumers))
    print('Making directory... {}'.format(mkdir_path))

    os.makedirs(mkdir_path, exist_ok=True)


if __name__ == '__main__':
    scripts_dir = os.path.dirname(os.path.realpath(__file__))
    brokers = get_hostnames(os.path.join(scripts_dir, BROKERS_FILE))
    consumers = get_hostnames(os.path.join(scripts_dir, CONSUMERS_FILE))
    producers = get_hostnames(os.path.join(scripts_dir, PRODUCERS_FILE))

    mkdir_benchmark_results(1, 1, 1, 5)
