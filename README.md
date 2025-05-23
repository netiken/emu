# emu

## Example

To run `emu` on your local machine, run each of the following commands in separate terminals:

```bash
# Start the server
cargo run --release manager --port 50000

# Start the workers (however many you want)
cargo run --release worker \
    --id 0 \
    --advertise-ip 127.0.0.1 \
    --control-port 50001 \
    --data-port 50002 \
    --manager-addr 127.0.0.1:50000 \
    --metrics-addr 0.0.0.0:9001
cargo run --release worker \
    --id 1 \
    --advertise-ip 127.0.0.1 \
    --control-port 50002 \
    --data-port 50003 \
    --manager-addr 127.0.0.1:50000 \
    --metrics-addr 0.0.0.0:9002

# Start Prometheus
prometheus --config.file examples/prometheus.yml

# Start a workload
cargo run --release run --spec examples/single.json --manager-addr 127.0.0.1:50000

# Stop the workload (required before starting another run)
cargo run --release stop --manager-addr 127.0.0.1:50000
```

You must stop a workload before starting a new `run`.
