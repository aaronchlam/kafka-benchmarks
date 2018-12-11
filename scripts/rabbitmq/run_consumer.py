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
               "-u {queue_name} " + \
               "-x 0 -y {num_consumers} " + \
               "-s {record_size} " + \
               "-D {total_records} " + \
               "--predeclared -ms "


def run_consumer(user, password, host, queue_name, num_consumers, record_size, total_records, throughput, output):
    tz = timezone(TIMEZONE)
    cmd = CMD_TEMPLATE.format(user=user, password=password, host=host,
                              num_consumers=num_consumers,
                              queue_name=queue_name,
                              record_size=record_size,
                              total_records=total_records)
    if throughput > 0:
        records_per_second = int(throughput / record_size)
        cmd += "-R {records_per_second} ".format(records_per_second=records_per_second)
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
    parser.add_argument("--queue", type=str, required=True)
    parser.add_argument("--num-consumers", type=int, required=True)
    parser.add_argument("--record-size", type=str, required=True, help="e.g. 256B")
    parser.add_argument("--total-records", type=int, required=True)
    parser.add_argument("--throughput", type=str, default="0MB", required=False, help="e.g. 10MB")
    parser.add_argument("--output", type=str, required=True)

    args = parser.parse_args()

    throughput = int(bitmath.parse_string(args.throughput).to_Byte())
    record_size = int(bitmath.parse_string(args.record_size).to_Byte())

    run_consumer(args.user, args.password, args.host, args.queue, args.num_consumers, record_size, args.total_records,
                 throughput, args.output)
