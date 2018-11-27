import os
import subprocess
import socket
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
RUN_CONSUMER_BENCHMARK_TEMPLATE = 'benchmarks/run_consumer_benchmark.py --topic {topic} --fetch-size {size} ' + \
                                  '--throughput {throughput} --time {time} --instances {instances} ' + \
                                  '--output {output} --broker {broker} --zookeeper {zookeeper}'


BENCHMARK_TOPIC = 'new-benchmark'
BENCHMARK_LENGTH = 360   # TODO: fix this
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


def run_producer_benchmark_script(producers, producer_throughput, zookeeper, data_dir):
    throughput_string = '{}MB'.format(producer_throughput)
    output_path = os.path.join(data_dir, 'producer.txt')    # TODO: generalize here for more producers
    py_cmd = RUN_PRODUCER_BENCHMARK_TEMPLATE.format(topic=BENCHMARK_TOPIC, size=RECORD_SIZE,
                                                    throughput=throughput_string, time=BENCHMARK_LENGTH,
                                                    instances=1, zookeeper=zookeeper, output=output_path,
                                                    config=PRODUCER_CONFIG)
    ssh_cmds = SSH_NODE_PY_CMD_TEMPLATE.format(py_cmd=py_cmd)

    clients = {}
    stds = {}
    for producer in producers:
        clients[producer] = open_ssh(producer)
        stds[producer] = clients[producer].exec_command(ssh_cmds)

    return clients, stds


def run_consumer_benchmark_script(consumers, instances, producer_throughput, broker, zookeeper, data_dir):
    broker_ip = socket.gethostbyname(broker)
    throughput_string = '{}MB'.format(producer_throughput)

    clients = {}
    stds = {}

    for consumer in consumers:
        output_path = os.path.join(data_dir, 'consumer-{}.txt'.format(consumer))
        py_cmd = RUN_CONSUMER_BENCHMARK_TEMPLATE.format(topic=BENCHMARK_TOPIC, size=RECORD_SIZE, time=BENCHMARK_LENGTH,
                                                        throughput=throughput_string, zookeeper=zookeeper,
                                                        output=output_path, instances=instances,
                                                        broker='{}:9092'.format(broker_ip))
        ssh_cmds = SSH_NODE_PY_CMD_TEMPLATE.format(py_cmd=py_cmd)
        print(ssh_cmds)

        clients[consumer] = open_ssh(consumer)
        stds[consumer] = clients[consumer].exec_command(ssh_cmds)

    return clients, stds


def run_producer_throughput_trial(zookeeper, trial, brokers, producers, consumers, consumer_instances, producer_throughput):
    # create test result directory
    data_dir = mkdir_benchmark_results(len(brokers), len(producers), len(consumers), producer_throughput, trial)

    # create kafka topic
    create_topic(zookeeper, BENCHMARK_TOPIC, replication_factor=len(brokers))

    # start vmstat
    start_vmstats(brokers, data_dir)

    # run the producer_benchmark_scripts
    producer_clients, producer_stds = run_producer_benchmark_script(producers, producer_throughput, zookeeper, data_dir)

    # run the consumer_benchmark_scripts
    if consumer_instances > 0:
        print('consumer instances: {}'.format(consumer_instances))
        consumer_clients, consumer_stds = run_consumer_benchmark_script(consumers, consumer_instances, producer_throughput, brokers[0],
                                                                    zookeeper, data_dir)

    for producer in producers:
        print("wiating on producers to finish")
        exit_status = producer_stds[producer][1].channel.recv_exit_status()
        print("producer exist_status: {}".format(exit_status))
        client = producer_clients[producer].close()
        print("done producers")

    if consumer_instances > 0:
        for consumer in consumers:
            print("waiting on consumers to finish")
            exit_status = consumer_stds[consumer][1].channel.recv_exit_status()
            print("consumer exit status: {}".format(exit_status))

            client = consumer_clients[consumer].close()

    # end vmstat
    stop_vmstats(brokers)

    # delete kafka topic
    delete_topic(zookeeper, BENCHMARK_TOPIC)


def run_experiments(zookeepers, brokers, consumers, producers):
    start_throughput = 5
    end_throughput = 60
    step_throughput = 5
    num_trials = 5
    consumer_instances = 1
    for throughput in range(start_throughput, end_throughput, step_throughput):
        print("\n========= RUNNING EXPERIMENT! ============\n")
        print("number of brokers: {}".format(len(brokers)))
        print("number of producers: {}".format(len(producers)))
        print("number_of consumer: {}".format(len(consumers)))
        print("number of instances per consumer: {}".format(consumer_instances))
        print("THROUGHPUT: {}MB".format(throughput))
        for trial in range(num_trials):
            print("\n---- TRIAL {} -----".format(trial))
            run_producer_throughput_trial(zookeepers[0], trial, brokers, producers, consumers, consumer_instances, throughput)
            print("------------------\n\n")


def run_increasing_clients_trial(zookeeper, trial, brokers, producers, consumers, num_clients):
    # create test result directory
    data_dir = mkdir_increasing_consumers_dir(len(brokers), num_clients, trial)

    # create kafka topic
    create_topic(zookeeper, BENCHMARK_TOPIC, replication_factor=len(brokers))

    # start vmstat
    start_vmstats(brokers, data_dir)

    # run the producer_benchmark_scripts
    producer_clients, producer_stds = run_producer_benchmark_script(producers, 50, zookeeper, data_dir)

    # run the consumer_benchmark_scripts
    num_clients_per_consumer = int(num_clients / len(consumers))
    if num_clients_per_consumer > 0:
        consumer_clients, consumer_stds = run_consumer_benchmark_script(consumers, num_clients_per_consumer, 50, brokers[0],
                                                                        zookeeper, data_dir)

    for producer in producers:
        print("wiating on producers to finish")
        exit_status = producer_stds[producer][1].channel.recv_exit_status()
        print("producer exist_status: {}".format(exit_status))
        client = producer_clients[producer].close()
        print("done producers")

    if num_clients_per_consumer > 0:
        for consumer in consumers:
            print("waiting on consumers to finish")
            exit_status = consumer_stds[consumer][1].channel.recv_exit_status()
            print("consumer exit status: {}".format(exit_status))

            client = consumer_clients[consumer].close()

    # end vmstat
    stop_vmstats(brokers)

    # delete kafka topic
    delete_topic(zookeeper, BENCHMARK_TOPIC)


def mkdir_increasing_consumers_dir(num_replicas, num_clients, trial):
    mkdir_path = os.path.join(DATA_DIR, 'increasing-clients', '{}-replicas'.format(num_replicas),
                              '{}-clients'.format(num_clients), '{}-trial'.format(trial))
    print('Making directory... {}'.format(mkdir_path))

    os.makedirs(mkdir_path, exist_ok=True)

    return mkdir_path


def run_increasing_consumers_experiment(zookeepers, brokers, consumers, producers):
    start_clients = 0
    end_clients = 65
    step_clients = 5
    num_trials = 5
    for clients in range(start_clients, end_clients, step_clients):
        print("\n========= RUNNING EXPERIMENT! ============\n")
        print("number of brokers: {}".format(len(brokers)))
        print("number of producers: {}".format(len(producers)))
        print("number_of consumer: {}".format(len(consumers)))
        print("NUMBER OF CLIENTS: {}".format(clients))
        for trial in range(num_trials):
            print("\n---- TRIAL {} -----".format(trial))
            run_increasing_clients_trial(zookeepers[0], trial, brokers, producers, consumers, clients)
            print("------------------\n\n")


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

    # run_experiments(zookeepers, brokers, consumers, producers)
    run_increasing_consumers_experiment(zookeepers, brokers, consumers, producers)