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
EXCHANGE_NAME = 'benchmark-exchange'
EXCHANGE_TYPE = 'fanout'
QUEUE_PATTERN = 'benchmark-queue-%d'
RECORD_SIZE = '512B'
TOTAL_RECORDS = 10000000

SSH_NODE_PY_CMD_TEMPLATE = '''
cd kafka-benchmarks/;
. venv/bin/activate;
{py_cmd}
'''
RUN_PRODUCER_TEMPLATE = 'run_producer.py --user {user} --password {password} --host {host} ' + \
                        '--exchange {exchange_name} --exchange-type {exchange_type} ' + \
                        '--queue-pattern {queue_pattern} --num-producers {num_producers} ' \
                        '--num-consumers {num_consumers} --record-size {record_size} ' + \
                        '--total-records {total_records} --throughput {throughput} --output {output} '
RUN_CONSUMER_TEMPLATE = 'run_consumer.py --user {user} --password {password} --host {host} --queue {queue} ' + \
                        '--num-consumers {num_consumers} --record-size {record_size} ' + \
                        '--total-records {total_records} --output {output} '


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


def mkdir_benchmark_results(num_nodes, num_producers, num_consumers, producer_throughput, trial, persistent=False):
    # TODO: generalize for not just producer throughput
    if not persistent:
        mkdir_path = os.path.join(DATA_DIR, '{}-nodes'.format(num_nodes),
                                  '{}-producers'.format(num_producers), '{}-consumers'.format(num_consumers),
                                  '{}-throughput'.format(producer_throughput), '{}-trial'.format(trial))
    else:
        mkdir_path = os.path.join(DATA_DIR, 'persistent', '{}-nodes'.format(num_nodes),
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


def run_producer_script(nodes, producers, num_instances, num_consumers, total_records, producer_throughput, data_dir,
                        persistent=False):
    throughput_string = '{}MB'.format(producer_throughput)
    clients = {}
    stds = {}
    for producer in producers:
        print("producer: {}".format(producer))
        output_path = os.path.join(data_dir, 'producer-{}.txt'.format(producer))    # TODO: generalize here for more producers
        py_cmd = RUN_PRODUCER_TEMPLATE.format(user=USER, password=PASSWORD, host=nodes[0], exchange_name=EXCHANGE_NAME,
                                              exchange_type=EXCHANGE_TYPE, queue_pattern=QUEUE_PATTERN,
                                              num_producers=num_instances, num_consumers=num_consumers,
                                              record_size=RECORD_SIZE, total_records=total_records,
                                              throughput=throughput_string, output=output_path)
        if persistent:
            py_cmd += "--persistent"
        ssh_cmds = SSH_NODE_PY_CMD_TEMPLATE.format(py_cmd=py_cmd)
        print(ssh_cmds)

        clients[producer] = open_ssh(producer)
        stds[producer] = clients[producer].exec_command(ssh_cmds)

    return clients, stds


def run_consumer_script(nodes, consumers, num_instances, total_records, data_dir, persistent=False):
    clients = {}
    stds = {}
    records_per_consumer = total_records // (len(consumers) * num_instances)
    for idx, consumer in enumerate(consumers):
        output_path = os.path.join(data_dir, 'consumer-{}.txt'.format(consumer))    # TODO: generalize here for more producers
        print('output_path: {}'.format(output_path))
        py_cmd = RUN_CONSUMER_TEMPLATE.format(user=USER, password=PASSWORD, host=nodes[0], queue=QUEUE_PATTERN % (idx),
                                              num_consumers=num_instances, record_size=RECORD_SIZE,
                                              total_records=records_per_consumer, output=output_path)
        if persistent:
            py_cmd += '--persistent '
        ssh_cmds = SSH_NODE_PY_CMD_TEMPLATE.format(py_cmd=py_cmd)
        print(ssh_cmds)

        clients[consumer] = open_ssh(consumer)
        stds[consumer] = clients[consumer].exec_command(ssh_cmds)

    return clients, stds


def run_trial(trial_num, nodes, consumers, producers, consumer_instances, producer_instances, producer_throughput,
              persistent=False):
    # create test result directory
    data_dir = mkdir_benchmark_results(len(nodes), len(producers), len(consumers), producer_throughput, trial_num,
                                       persistent=persistent)

    # start vmstat & iostat
    start_vmstats(nodes, data_dir)
    start_iostats(nodes, data_dir)

    # run the run_consumer.py script
    consumer_clients, consumer_stds = run_consumer_script(nodes, consumers, consumer_instances, TOTAL_RECORDS, data_dir,
                                                          persistent=persistent)

    # run the run_producer.py script
    producer_clients, producer_stds = run_producer_script(nodes, producers, producer_instances, len(consumers),
                                                          TOTAL_RECORDS, producer_throughput, data_dir,
                                                          persistent=persistent)

    for producer in producers:
        print("waiting on producers {} to finish".format(producer))
        exit_status = producer_stds[producer][1].channel.recv_exit_status()
        print("producer exist_status: {}".format(exit_status))
        client = producer_clients[producer].close()

    print("done producers")

    if consumer_instances > 0:
        for consumer in consumers:
            if consumer in consumer_clients:
                print("waiting on consumer {} to finish".format(consumer))
                exit_status = consumer_stds[consumer][1].channel.recv_exit_status()
                print("consumer exit status: {}".format(exit_status))

                client = consumer_clients[consumer].close()

    # end vmstat & iostat
    stop_vmstats(nodes)
    stop_iostats(nodes)


def run_experiments(nodes, consumers, producers):
    start_throughput = 1
    end_throughput = 15
    step_throughput = 1
    num_trials = 1
    for throughput in range(start_throughput, end_throughput, step_throughput):
        print("\n========= RUNNING EXPERIMENT! ============\n")
        print("number of nodes: {}".format(len(nodes)))
        print("number of producers: {}".format(len(producers)))
        print("number_of consumer: {}".format(len(consumers)))
        print("THROUGHPUT: {}MB".format(throughput))
        for trial in range(num_trials):
            print("\n---- TRIAL {} -----".format(trial))
            run_trial(trial, nodes, consumers, producers, 1, 1, throughput, persistent=False)    # TODO: switch the persistent flag here
            print("------------------\n\n")


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

