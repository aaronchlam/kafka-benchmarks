import argparse
import os
import subprocess
import time

import bitmath
from run_producer import run_producer_script as run_producer

CMD_TEMPLATE = "kafka-topics.sh {cmd} --zookeeper {zookeeper}  {options_str}"

CREATE_OPT_TEMPLATE = "--replication-factor {replication_factor} " + \
                      "--partitions {partitions} " + \
                      "--topic {topic}"
DELETE_OPT_TEMPLATE = "--topic {topic}"


def create_topic(zookeeper, topic, replication_factor=1, partitions=1):
    options_str = CREATE_OPT_TEMPLATE.format(
        replication_factor=replication_factor,
        partitions=partitions,
        topic=topic)
    subprocess.call(CMD_TEMPLATE.format(cmd="--create",
                                        zookeeper=zookeeper,
                                        options_str=options_str), shell=True)


def delete_topic(zookeeper, topic):
    options_str = DELETE_OPT_TEMPLATE.format(topic=topic)
    subprocess.call(CMD_TEMPLATE.format(cmd="--delete",
                                        zookeeper=zookeeper,
                                        options_str=options_str), shell=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", type=str, required=True)
    parser.add_argument("--record-size", type=str, required=True, help="e.g. 256B")
    parser.add_argument("--throughput", type=str, required=True, help="e.g. 10MB")
    parser.add_argument("--time", type=int, required=True,
                        help="time in seconds, e.g. 60")
    parser.add_argument("--instances", type=int, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--producer-config", type=str, required=True)
    parser.add_argument("--zookeeper", type=str, required=True)

    args = parser.parse_args()

    throughput = int(bitmath.parse_string(args.throughput).to_Byte())
    record_size = int(bitmath.parse_string(args.record_size).to_Byte())

    records_per_second = throughput / record_size
    total_records = int(records_per_second * args.time)

    processes = []

    print("Running producer benchmark")

    if args.instances == 1:
        print("throughput: {}".format(throughput))
        print("record_siz: {}".format(record_size))
        print("records_per_second: {}".format(records_per_second))
        print("total_records: {}".format(total_records))
        processes.append(run_producer(args.topic, throughput, record_size,
                                      total_records, args.producer_config, args.output))
    else:
        dir_path, basename = os.path.split(args.output)
        root, ext = os.path.splitext(basename)
        for i in range(args.instances):
            output_name = "{root}-{instance}{ext}".format(root=root,
                                                          instance=i, ext=ext)
            output_path = os.path.join(dir_path, output_name)
            processes.append(run_producer(args.topic, throughput, record_size,
                                          total_records, args.producer_config, output_path))

    for p in processes:
        exit_code = p.wait()
        print("Exit code: {}".format(exit_code))

    print("Sleeping in the producer benchmark")
    time.sleep(5)
    print("Done sleeping")
