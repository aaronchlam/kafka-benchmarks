#!/usr/bin/env python

import argparse
import bitmath
import subprocess
import os
from datetime import datetime
from pytz import timezone

TIMEZONE = 'EST'

CMD_TEMPLATE = "runjava com.rabbitmq.perf.PerfTest " \
               "-h amqp://{user}:{password}@{host} " + \
               "-e {exchange_name} " + \
               "-t {exchange_type} " + \
               "-x {num_producers} -y 0 " + \
               "-qp {queue_pattern} " + \
               "-qpf {queue_start} " + \
               "-qpt {queue_end} " + \
               "-s {record_size} " + \
               "-C {total_records} " + \
               "-r {records_per_second} -ms "


def run_producer(user, password, host, exchange_name, exchange_type, queue_pattern, num_producers, num_consumers,
                 record_size, total_records, throughput, output, persistent=False):
    tz = timezone(TIMEZONE)
    records_per_second = int(throughput / record_size)
    cmd = CMD_TEMPLATE.format(user=user, password=password, host=host,
                              exchange_name=exchange_name,
                              exchange_type=exchange_type,
                              num_producers=num_producers,
                              queue_pattern=queue_pattern,
                              queue_start=0, queue_end=num_consumers - 1,
                              record_size=record_size,
                              total_records=total_records,
                              records_per_second=records_per_second)
    if persistent:
        cmd += "-f persistent"
    with open(os.path.abspath(output), 'w') as output_file:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True, shell=True)
        for line in p.stdout:
            now = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
            output_file.write('{}, {}'.format(now, line))
    return p


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", type=str, required=True)
    parser.add_argument("--password", type=str, required=True)
    parser.add_argument("--host", type=str, required=True)
    parser.add_argument("--exchange", type=str, required=True)
    parser.add_argument("--exchange-type", type=str, required=True)
    parser.add_argument("--queue-pattern", type=str, required=True)
    parser.add_argument("--num-producers", type=int, required=True)
    parser.add_argument("--num-consumers", type=int, required=True)
    parser.add_argument("--record-size", type=str, required=True, help="e.g. 256B")
    parser.add_argument("--total-records", type=int, required=True)
    parser.add_argument("--throughput", type=str, required=True, help="e.g. 10MB")
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--persistent", dest='persistent', action='store_true')

    args = parser.parse_args()

    throughput = int(bitmath.parse_string(args.throughput).to_Byte())
    record_size = int(bitmath.parse_string(args.record_size).to_Byte())

    p = run_producer(args.user, args.password, args.host, args.exchange, args.exchange_type, args.queue_pattern,
                     args.num_producers, args.num_consumers, record_size, args.total_records, throughput, args.output,
                     args.persistent)

    exit_code = p.wait()
