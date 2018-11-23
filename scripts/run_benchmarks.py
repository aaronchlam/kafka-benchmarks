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

VMSTAT_START_CMD = 'vmstat -n -t -S M 1 3600 > {path} &'

SSH_NODE_PY_CMD_TEMPLATE = '''
cd kafka-benchmarks/;
. venv/bin/activate;
python {py_cmd}
'''
RUN_PRODUCER_BENCHMARK_TEMPLATE = 'benchmarks/run_producer_benchmark.py --topic {topic} --record-size {size} ' + \
                                  '--throughput {throughput} --time {time} --instances {instances} ' + \
                                  '--producer-config {config} --zookeeper {zookeeper} --output {output}'


BENCHMARK_TOPIC = 'benchmark-topic'
BENCHMARK_LENGTH = 60   # 5 minutes
RECORD_SIZE = '512B'

CONFIG_DIR = os.path.join(os.getcwd(), 'config')
PRODUCER_CONFIG = os.path.join(CONFIG_DIR, 'producer.properties')


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
        vmstat_file_path = os.path.join(data_dir, 'vmstat_{}.txt'.format(host))
        stdin, stdout, stderr = client.exec_command(VMSTAT_START_CMD.format(path=vmstat_file_path))
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


def run_producer_benchmark_script(producers, producer_throughput, zookeeper, data_dir):
    throughput_string = '{}MB'.format(producer_throughput)
    output_path = os.path.join(data_dir, 'producer.txt')
    py_cmd = RUN_PRODUCER_BENCHMARK_TEMPLATE.format(topic=BENCHMARK_TOPIC, size=RECORD_SIZE,
                                                    throughput=throughput_string, time=BENCHMARK_LENGTH,
                                                    instances=1, zookeeper=zookeeper, output=output_path,
                                                    config=PRODUCER_CONFIG)
    ssh_cmds = SSH_NODE_PY_CMD_TEMPLATE.format(py_cmd=py_cmd)
    print(ssh_cmds)

    clients = {}
    stds = {}
    for producer in producers:
        clients[producer] = open_ssh(producer)
        stds[producer] = clients[producer].exec_command(SSH_NODE_PY_CMD_TEMPLATE.format(py_cmd=py_cmd))

    return clients, stds


def run_producer_throughput_trial(zookeeper, trial, brokers, producers, consumers, producer_throughput):
    # create test result directory
    data_dir = mkdir_benchmark_results(len(brokers), len(producers), len(consumers), producer_throughput, trial)

    # create kafka topic
    create_topic(zookeeper, BENCHMARK_TOPIC, replication_factor=len(brokers))

    # start vmstat
    start_vmstats(brokers, data_dir)

    # run the producer_benchmark_scripts
    producer_clients, producer_stds = run_producer_benchmark_script(producers, producer_throughput, zookeeper, data_dir)

    # run the consumer_benchmark_scripts

    for producer in producers:
        exit_status = producer_stds[producer][1].channel.recv_exit_status()
        print('producer {} exit code: {}'.format(producer, exit_status))
        client = producer_clients[producer].close()

    # end vmstat
    stop_vmstats(brokers)

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

    run_producer_throughput_trial(zookeepers[0], 0, brokers, producers, consumers, 5)
    # run_producer_benchmark_script(producers, 5, 'tem07', '/bullshit/dir')

