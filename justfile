default:
    @just --list

kill-local:
    sudo pkill emu || true
    sudo pkill prometheus || true

start-local:
    @just kill-local
    prometheus --config.file examples/prometheus.yml >/dev/null 2>&1 &
    cargo run --release manager --port 50000 >/dev/null 2>&1 &
    cargo run --release worker --id 0 \
        --advertise-ip 127.0.0.1 --control-port 50001 --data-port 50005 \
        --manager-addr 127.0.0.1:50000 --metrics-addr 0.0.0.0:9001 \
        >worker0.log 2>&1 &
    cargo run --release worker --id 1 \
        --advertise-ip 127.0.0.1 --control-port 50002 --data-port 50006 \
        --manager-addr 127.0.0.1:50000 --metrics-addr 0.0.0.0:9002 \
        >worker1.log 2>&1 &
    cargo run --release worker --id 2 \
        --advertise-ip 127.0.0.1 --control-port 50003 --data-port 50007 \
        --manager-addr 127.0.0.1:50000 --metrics-addr 0.0.0.0:9003 \
        >worker2.log 2>&1 &
    cargo run --release worker --id 3 \
        --advertise-ip 127.0.0.1 --control-port 50004 --data-port 50008 \
        --manager-addr 127.0.0.1:50000 --metrics-addr 0.0.0.0:9004 \
        >worker3.log 2>&1 &
