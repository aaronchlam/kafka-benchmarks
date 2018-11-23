import os
import subprocess
import time

import paramiko

DATA_DIR = '/home/achlam/kafka-benchmarks/data'

USERNAME = 'achlam'

ZOOKEEPERS_FILE = 'zookeepers'
BROKERS_FILE = 'brokers'
CONSUMERS_FILE = 'consumers'
PRODUCERS_FILE = 'producers'

KAKFA_TOPICS_CMD_TEMPLATE = "kafka-topics.sh {cmd} --zookeeper {zookeeper}  {options_str}"

CREATE_OPT_TEMPLATE = "--replication-factor {replication_factor} " + \
                      "--partitions {partitions} " + \
                      "--topic {topic}"
DELETE_OPT_TEMPLATE = "--topic {topic}"

VMSTAT_START_CMD = 'vmstat -n -t -S M 1 3600 > {path}'


BENCHMARK_TOPIC = 'benchmark-topic'
BENCHMARK_LENTH = '300' # 5 minutes


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


def mkdir_benchmark_results(num_replicas, num_producers, num_consumers, producer_throughput, trial):
    # TODO: generalize for not just producer throughput
    mkdir_path = os.path.join(DATA_DIR, '{}-replicas'.format(num_replicas),
                              '{}-producers'.format(num_producers), '{}-consumers'.format(num_consumers),
                              '{}-throughput'.format(producer_throughput), '{}-trial'.format(trial))
    print('Making directory... {}'.format(mkdir_path))

    os.makedirs(mkdir_path, exist_ok=True)

    return mkdir_path


def create_topic(zookeeper, topic, replication_factor=1, partitions=1):
    options_str = CREATE_OPT_TEMPLATE.format(
        replication_factor=replication_factor,
        partitions=partitions,
        topic=topic)
    subprocess.call(KAKFA_TOPICS_CMD_TEMPLATE.format(cmd="--create",
                                                     zookeeper=zookeeper,
                                                     options_str=options_str), shell=True)


def delete_topic(zookeeper, topic):
    options_str = DELETE_OPT_TEMPLATE.format(topic=topic)
    subprocess.call(KAKFA_TOPICS_CMD_TEMPLATE.format(cmd="--delete",
                                                     zookeeper=zookeeper,
                                                     options_str=options_str), shell=True)


def start_vmstats(hosts, data_dir):
    channels = []

    for host in hosts:
        client = open_ssh(host)
        stdin, stdout, stderr = client.exec_command(VMSTAT_START_CMD.format(data_dir))
        channels.append(stdout.channel)

    for channel in channels:
        exit_status = channel.recv_exit_status()
        client.close()


def stop_vmstats(hosts):
    channels = []

    for host in hosts:
        client = open_ssh(host)
        stdin, stdout, stderr = client.exec_command('pkill -f vmstat')
        channels.append(stdout.channel)

    for channel in channels:
        exit_status = channel.recv_exit_status()
        client.close()


def run_producer_throughput_trial(zookeeper, trial, num_replicas, producers, consumers, producer_throughput):
    # create test result directory
    data_dir = mkdir_benchmark_results(num_replicas, len(producers), len(consumers), producer_throughput, trial)

    # create kafka topic
    create_topic(zookeeper, BENCHMARK_TOPIC, replication_factor=num_replicas)

    # start vmstat
    start_vmstats(producers, data_dir)

    time.sleep(20)

    # end vmstat
    stop_vmstats(producers)

    # delete kafka topic
    delete_topic(zookeeper, BENCHMARK_TOPIC)


if __name__ == '__main__':
    scripts_dir = os.path.dirname(os.path.realpath(__file__))
    zookeepers = get_hostnames(os.path.join(scripts_dir, ZOOKEEPERS_FILE))
    brokers = get_hostnames(os.path.join(scripts_dir, BROKERS_FILE))
    consumers = get_hostnames(os.path.join(scripts_dir, CONSUMERS_FILE))
    producers = get_hostnames(os.path.join(scripts_dir, PRODUCERS_FILE))

    print('zookeepers: {}'.format(zookeepers))
    print('brokers: {}'.format(brokers))
    print('consumers: {}'.format(consumers))
    print('producers: {}'.format(producers))

    run_producer_throughput_trial(zookeepers[0], 0, 1, producers, consumers, 5)

