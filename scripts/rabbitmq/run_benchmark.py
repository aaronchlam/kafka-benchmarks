import os
import subprocess
import socket
import time

import paramiko

DATA_DIR = '/home/achlam/kafka-benchmarks/data/rabbitmq'

USERNAME = 'achlam'

VMSTAT_START_CMD = 'vmstat -n -t -S M 1 3600 > {path} &'
IOSTAT_START_CMD = 'iostat -xmt 1 3600 > {path} &'

NODES_FILE = 'nodes'
CONSUMERS_FILE = 'consumers'
PRODUCERS_FILE = 'producers'

USER = 'admin'
PASSWORD = 'password'
QUEUE_NAME = 'benchmark-queue'
RECORD_SIZE = '512B'
TOTAL_RECORDS = '100000000'

SSH_NODE_PY_CMD_TEMPLATE = '''
cd kafka-benchmarks/;
. venv/bin/activate;
{py_cmd}
'''
RUN_PRODUCER_TEMPLATE = 'run_producer.py --user {user} --password {password} --host {host} --queue {queue} ' + \
                        '--num-producers {num_producers} --record-size {record_size} ' + \
                        '--total-records {total_records} --throughput {throughput} --output {output} '
RUN_CONSUMER_TEMPLATE = 'run_producer.py --user {user} --password {password} --host {host} --queue {queue} ' + \
                        '--num-producers {num_producers} --record-size {record_size} ' + \
                        '--total-records {total_records} --throughput {throughput} --output {output} '


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


def mkdir_benchmark_results(num_nodes, num_producers, num_consumers, producer_throughput, trial):
    # TODO: generalize for not just producer throughput
    mkdir_path = os.path.join(DATA_DIR, '{}-nodes'.format(num_nodes),
                              '{}-producers'.format(num_producers), '{}-consumers'.format(num_consumers),
                              '{}-throughput'.format(producer_throughput), '{}-trial'.format(trial))
    print('Making directory... {}'.format(mkdir_path))

    os.makedirs(mkdir_path, exist_ok=True)

    return mkdir_path


def start_vmstats(hosts, data_dir):
    channels = []

    for host in hosts:
        client = open_ssh(host)
        vmstat_file_path = os.path.join(data_dir, 'vmstat-{}.txt'.format(host))
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


def start_iostats(hosts, data_dir):
    channels = []

    for host in hosts:
        client = open_ssh(host)
        iostat_file_path = os.path.join(data_dir, 'iostat-{}.txt'.format(host))
        stdin, stdout, stderr = client.exec_command(IOSTAT_START_CMD.format(path=iostat_file_path))
        channels.append(stdout.channel)

    for channel in channels:
        exit_status = channel.recv_exit_status()

    client.close()


def stop_iostats(hosts):
    channels = []

    for host in hosts:
        client = open_ssh(host)
        stdin, stdout, stderr = client.exec_command('pkill -f iostat')
        channels.append(stdout.channel)

    for channel in channels:
        exit_status = channel.recv_exit_status()

    client.close()


def run_producer_script(producers, num_instances, total_records, producer_throughput, data_dir):
    throughput_string = '{}MB'.format(producer_throughput)
    clients = {}
    stds = {}
    for producer in producers:
        print("producer: {}".format(producer))
        output_path = os.path.join(data_dir, 'producer-{}.txt'.format(producer))    # TODO: generalize here for more producers
        py_cmd = RUN_PRODUCER_TEMPLATE.format(user=USER, password=PASSWORD, host=producer, queue=QUEUE_NAME,
                                              num_producers=num_instances, record_size=RECORD_SIZE,
                                              total_records=total_records, throughput=throughput_string,
                                              output=output_path)
        ssh_cmds = SSH_NODE_PY_CMD_TEMPLATE.format(py_cmd=py_cmd)
        print(ssh_cmds)

        clients[producer] = open_ssh(producer)
        stds[producer] = clients[producer].exec_command(ssh_cmds)

    return clients, stds


def run_trial(trial_num, nodes, consumers, producers, num_instances, producer_throughput):
    # create test result directory
    data_dir = mkdir_benchmark_results(len(nodes), len(consumers), len(producers), producer_throughput, trial_num)

    # start vmstat & iostat
    start_vmstats(nodes, data_dir)
    start_iostats(nodes, data_dir)

    # run the run_producer.py script
    producer_clients, producer_stds = run_producer_script(producers, num_instances, TOTAL_RECORDS, producer_throughput, data_dir)

    # run the consumer_benchmark_scripts
    #if num_clients > 0:
    #    consumer_clients, consumer_stds = run_consumer_benchmark_script(consumers, num_clients, 50, brokers[0],
    #                                                                    zookeeper, data_dir)

    for producer in producers:
        print("waiting on producers {} to finish".format(producer))
        exit_status = producer_stds[producer][1].channel.recv_exit_status()
        print("producer exist_status: {}".format(exit_status))
        client = producer_clients[producer].close()

    print("done producers")

    #if num_clients > 0:
    #    for consumer in consumers:
    #        if consumer in consumer_clients:
    #            print("waiting on consumer {} to finish".format(consumer))
    #            exit_status = consumer_stds[consumer][1].channel.recv_exit_status()
    #            print("consumer exit status: {}".format(exit_status))

    #            client = consumer_clients[consumer].close()

    # end vmstat & iostat
    stop_vmstats(nodes)
    stop_iostats(nodes)


def run_experiments(nodes, consumers, producers):
    run_trial(0, nodes, consumers, producers, 1, 20)


if __name__ == '__main__':
    scripts_dir = os.path.dirname(os.path.realpath(__file__))
    nodes = get_hostnames(os.path.join(scripts_dir, NODES_FILE))
    consumers = get_hostnames(os.path.join(scripts_dir, CONSUMERS_FILE))
    producers = get_hostnames(os.path.join(scripts_dir, PRODUCERS_FILE))

    print('nodes: {}'.format(nodes))
    print('consumers: {}'.format(consumers))
    print('producers: {}'.format(producers))

    # run_experiments(zookeepers, brokers, consumers, producers)
    run_experiments(nodes, consumers, producers)

