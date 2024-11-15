default:
    @just --list

kill-local:
    sudo pkill emu || true
    sudo pkill prometheus || true

start-local:
    @just kill-local
    prometheus --config.file examples/prometheus.yml >/dev/null 2>&1 &
    cargo run --release manager --port 50000 >/dev/null 2>&1 &
    cargo run --release worker --id 0 --advertise-addr 127.0.0.1:50001 --manager-addr 127.0.0.1:50000 --metrics-addr 0.0.0.0:9001 >/dev/null 2>&1 &
    cargo run --release worker --id 1 --advertise-addr 127.0.0.1:50002 --manager-addr 127.0.0.1:50000 --metrics-addr 0.0.0.0:9002 >/dev/null 2>&1 &
    cargo run --release worker --id 2 --advertise-addr 127.0.0.1:50003 --manager-addr 127.0.0.1:50000 --metrics-addr 0.0.0.0:9003 >/dev/null 2>&1 &
    cargo run --release worker --id 3 --advertise-addr 127.0.0.1:50004 --manager-addr 127.0.0.1:50000 --metrics-addr 0.0.0.0:9004 >/dev/null 2>&1 &
