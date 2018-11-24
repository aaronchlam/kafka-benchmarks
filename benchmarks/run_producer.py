import argparse
import bitmath
import subprocess
import os
from datetime import datetime
from pytz import timezone

TIMEZONE = 'EST'

CMD_TEMPLATE = "kafka-producer-perf-test.sh --topic {topic} " + \
               "--throughput {records_per_second} " + \
               "--record-size {record_size} " + \
               "--num-records {total_records} " + \
               "--producer.config {producer_config}"


def run_producer_script(topic, throughput, record_size, total_records,
                        producer_config, output):
    tz = timezone(TIMEZONE)
    records_per_second = int(throughput / record_size)
    cmd = CMD_TEMPLATE.format(topic=topic,
                              records_per_second=records_per_second,
                              record_size=record_size,
                              total_records=total_records,
                              producer_config=os.path.abspath(producer_config))
    with open(os.path.abspath(output), 'w') as output_file:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True, shell=True)
        for line in p.stdout:
            now = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
            output_file.write('{}, {}'.format(now, line))
    return p


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", type=str, required=True)
    parser.add_argument("--record-size", type=str, required=True, help="e.g. 256B")
    parser.add_argument("--throughput", type=str, required=True, help="e.g. 10MB")
    parser.add_argument("--time", type=int, required=True,
                        help="time in seconds, e.g. 60")
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--producer-config", type=str, required=True)

    args = parser.parse_args()

    throughput = int(bitmath.parse_string(args.throughput).to_Byte())
    record_size = int(bitmath.parse_string(args.record_size).to_Byte())

    records_per_second = throughput / record_size
    total_records = int(records_per_second * args.time)

    run_producer_script(args.topic, throughput, record_size, total_records,
                        args.producer_config, args.output)
