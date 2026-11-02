import sys

output_log_file = "logs.log"
purged_log_file = "purged_logs.log"

sort_by_prefix = "--sort-by-prefix" in sys.argv
rabbit_keywords = ("pika", "rabbitmq", "amqp", "ampq")

with open(output_log_file, "w") as f_raw, open(purged_log_file, "w") as f_purged:
    if not sort_by_prefix:
        for line in sys.stdin:
            f_raw.write(line)
            f_raw.flush()

            if any(kw in line.lower() for kw in rabbit_keywords):
                continue

            f_purged.write(line)
            f_purged.flush()

            sys.stdout.write(line)
            sys.stdout.flush()
    else:
        lines = sys.stdin.readlines()
        lines.sort(key=lambda x: x.split(" ")[0])
        for line in lines:
            f_raw.write(line)

            if any(kw in line.lower() for kw in rabbit_keywords):
                continue

            f_purged.write(line)
            sys.stdout.write(line)