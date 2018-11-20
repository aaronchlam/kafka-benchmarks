import argparse
import bitmath
import subprocess
import os
import time
from datetime import datetime as dt

from run_producer_benchmark import create_topic, delete_topic

REPORTING_INTERVAL = 5000 # Report every 5 seconds
TIMEOUT = 30000

CMD_TEMPLATE = "kafka-consumer-perf-test.sh --topic {topic} " + \
               "--broker-list {broker} " + \
               "--fetch-size {fetch_size} " + \
               "--from-latest " + \
               "--reporting-interval {reporting_interval} " + \
               "--messages {messages} " + \
               "--show-detailed-stats " + \
               "--timeout {timeout} "


def run_consumer_script(topic, broker, fetch_size, messages, reporting_interval,
        timeout, output):
    with open(os.path.abspath(output), 'w') as output_file:
        p = subprocess.Popen(CMD_TEMPLATE.format(topic=topic,
            broker=broker,
            fetch_size=fetch_size,
            reporting_interval=reporting_interval,
            messages=messages,
            timeout=timeout),
            stdout=output_file, shell=True)
    return p


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", type=str, required=True)
    parser.add_argument("--fetch-size", type=str, required=True, 
            help="e.g. 256B")
    parser.add_argument("--throughput", type=str, required=True, 
            help="e.g. 10MB")
    parser.add_argument("--time", type=int, required=True, 
            help="time in seconds, e.g. 60")
    parser.add_argument("--instances", type=int, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--broker", type=str, required=True)
    parser.add_argument("--zookeeper", type=str, required=True)

    args = parser.parse_args()

    throughput=int(bitmath.parse_string(args.throughput).to_Byte())
    fetch_size=int(bitmath.parse_string(args.fetch_size).to_Byte())

    records_per_second = throughput / fetch_size
    total_records = records_per_second * args.time

    create_topic(args.zookeeper, args.topic)

    time.sleep(10)

    processes = []

    print("Benchmark begins: {}".format(dt.now()))

    if args.instances == 1:
        processes.append(run_consumer_script(args.topic, args.broker, 
            fetch_size, total_records, REPORTING_INTERVAL, TIMEOUT, args.output))
    else:
        dir_path, basename = os.path.split(args.output)
        root, ext = os.path.splitext(basename)
        for i in range(args.instances):
            output_name = "{root}-{instance}{ext}".format(root=root,
                    instance=i, ext=ext)
            output_path = os.path.join(dir_path, output_name)
        processes.append(run_consumer_script(args.topic, args.broker, 
            fetch_size, total_records, REPORTING_INTERVAL, TIMEOUT, output_path))

    for p in processes:
        p.wait()

    print("Benchmark finished: {}".format(dt.now()))

