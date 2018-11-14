import argparse
import bitmath
import subprocess
import os

CONF_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config"))
CMD_TEMPLATE = "kafka-producer-perf-test.sh --topic {topic} " + 
               "--throughput {throughput} --record-size {record_size} " +
               "--num-records {num_records} --producer.config {producer_config}"

parser = argparse.ArgumentParser()
parser.add_argument("--topic", type=str, required=True)
parser.add_argument("--record-size", type=bithmath.integrations.BitmathType,
        required=True, help="e.g. 256B")
parser.add_argument("--throughput", type=bitmath.integrations.BitmathType,
        required=True, help="e.g. 10MB")
parser.add_argument("--output", type=str, required=True)

args = parser.parse_args()

with open(os.path.abspath(args.output)) as output_file:
    subprocess.call(CMD_TEMPLATE.format(topic=args.topic,
        throughput=int(args.throughput.to_Byte()),
        record_size=int(args.record_size.to_Byte()),
        num_records=args.num_records,
        producer_config=os.path.join(CONF_DIR, "producer.properties")), 
        stdout=output_file)
