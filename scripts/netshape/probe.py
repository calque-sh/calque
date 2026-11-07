"""Measurements of loopback TCP under calque-netshape.

usage:
  python3 probe.py witness <connects> <port>...
  python3 probe.py calibrate <Mbit/s> <RTT ms> <port> [<MiB> [<repeats>]]

`witness` connects <connects> times to the listener already on each <port>,
and as many times to an unshaped control listener of its own on an ephemeral
port. It prints one JSON object with each port's connect times and the MSS
the client settled on. `pnpm bench` judges it.

`calibrate` listens on <port> itself, so `up` must have named that port, and
times <repeats> transfers of <MiB> (50 by default) down from the port and up
to it, then one download from the control. It prints JSON with the verdict
under this tolerance:

- each direction's median second-half goodput lies within [0.90, 1.00] of
  <Mbit/s>. dummynet charges a packet's whole IP length against the rate, so
  exact shaping reads 0.965; the rest of the floor is for scheduling and
  dummynet's 1 ms timer on a loaded host;
- the fastest connect to the port, one crossing of each pipe, takes at least
  <RTT ms> - 2 ms: dummynet reads its clock in whole milliseconds, so each
  crossing can be up to 1 ms short of half the RTT;
- the port's MSS is at most 1460, the shaper's max-mss, and the control's is
  above it.

It runs under the system's /usr/bin/python3 (3.9) and needs only the standard
library.
"""
import json
import queue
import socket
import statistics
import sys
import threading
import time

MSS_CLAMP = 1460
CLOCK_MS = 1
CHUNK = b"\0" * (256 * 1024)


def connect(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(10)
    t0 = time.perf_counter()
    s.connect(("127.0.0.1", port))
    ms = (time.perf_counter() - t0) * 1000
    return s, ms, s.getsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG)


def listen(port):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", port))
    listener.listen(8)
    return listener, listener.getsockname()[1]


def handshakes(port, count):
    connects, mss = [], []
    for _ in range(count):
        s, ms, segment = connect(port)
        connects.append(round(ms, 3))
        mss.append(segment)
        s.close()
    return {"port": port, "connectMs": connects, "mss": mss}


def witness(count, ports):
    listener, control = listen(0)

    def accept():
        for _ in range(count):
            conn, _ = listener.accept()
            conn.close()

    acceptor = threading.Thread(target=accept, daemon=True)
    acceptor.start()
    shaped = [handshakes(port, count) for port in ports]
    reading = handshakes(control, count)
    acceptor.join(10)
    listener.close()
    return {"shaped": shaped, "control": reading}


def receive(conn, size):
    """Bytes received to EOF or size, and when the receiver held the half and the last."""
    got, half_at, half_got = 0, None, 0
    while got < size:
        data = conn.recv(1 << 20)
        if not data:
            break
        got += len(data)
        if half_at is None and got >= size // 2:
            half_at, half_got = time.perf_counter(), got
    return got, half_at, half_got, time.perf_counter()


def send(conn, size):
    sent = 0
    while sent < size:
        sent += conn.send(CHUNK[: min(len(CHUNK), size - sent)])


def serve(listener, size, uploads):
    while True:
        conn, _ = listener.accept()
        mode = conn.recv(1)
        if mode == b"q":
            conn.close()
            return
        if mode == b"d":
            send(conn, size)
            conn.shutdown(socket.SHUT_WR)
            conn.recv(1)
        elif mode == b"u":
            uploads.put(receive(conn, size))
            conn.sendall(b"k")
        conn.close()


def mbit(size, seconds):
    return size * 8 / seconds / 1e6


def download(port, size):
    s, ms, segment = connect(port)
    s.sendall(b"d")
    t0 = time.perf_counter()
    got, half_at, half_got, end = receive(s, size)
    s.sendall(b"x")
    s.close()
    if got != size or half_at is None:
        raise RuntimeError(f"download from port {port} received {got} of {size} bytes")
    return {
        "connectMs": round(ms, 3),
        "mss": segment,
        "wholeMbit": round(mbit(size, end - t0), 2),
        "secondHalfMbit": round(mbit(size - half_got, end - half_at), 2),
    }


def upload(port, size, uploads):
    s, ms, segment = connect(port)
    s.sendall(b"u")
    t0 = time.perf_counter()
    send(s, size)
    if s.recv(1) != b"k":
        raise RuntimeError(f"upload to port {port} was not acknowledged")
    end = time.perf_counter()
    s.close()
    got, half_at, half_got, last = uploads.get(timeout=10)
    if got != size or half_at is None:
        raise RuntimeError(f"upload to port {port} delivered {got} of {size} bytes")
    return {
        "connectMs": round(ms, 3),
        "mss": segment,
        "wholeMbit": round(mbit(size, end - t0), 2),
        "secondHalfMbit": round(mbit(size - half_got, last - half_at), 2),
    }


def server(port, size):
    listener, bound = listen(port)
    uploads = queue.Queue()
    thread = threading.Thread(target=serve, args=(listener, size, uploads), daemon=True)
    thread.start()
    return bound, uploads


def stop(port):
    s, _, _ = connect(port)
    s.sendall(b"q")
    s.close()


def calibrate(rate, rtt, port, mib, repeats):
    size = mib * 1024 * 1024
    _, uploads = server(port, size)
    downs, ups = [], []
    for _ in range(repeats):
        downs.append(download(port, size))
        ups.append(upload(port, size, uploads))
    stop(port)
    control, _ = server(0, size)
    control_down = download(control, size)
    stop(control)

    low, high = 0.90 * rate, 1.00 * rate
    floor = rtt - 2 * CLOCK_MS
    problems = []
    for name, transfers in (("down", downs), ("up", ups)):
        median = statistics.median(t["secondHalfMbit"] for t in transfers)
        if not low <= median <= high:
            problems.append(
                f"{name}: median second-half goodput {median:.2f} Mbit/s is outside [{low:.2f}, {high:.2f}]"
            )
    fastest = min(t["connectMs"] for t in downs + ups)
    if fastest < floor:
        problems.append(f"the fastest connect took {fastest:.3f} ms, under {floor} ms")
    for t in downs + ups:
        if t["mss"] > MSS_CLAMP:
            problems.append(f"a connect to port {port} settled on MSS {t['mss']}, above {MSS_CLAMP}")
            break
    if control_down["mss"] <= MSS_CLAMP:
        problems.append(f"the control settled on MSS {control_down['mss']}: the clamp reached an unshaped port")
    return {
        "megabitsPerSecond": rate,
        "rttMs": rtt,
        "port": port,
        "mebibytes": mib,
        "down": downs,
        "up": ups,
        "control": dict(control_down, port=control),
        "tolerance": {"goodputMbit": [low, high], "connectFloorMs": floor, "mssClamp": MSS_CLAMP},
        "problems": problems,
        "passed": not problems,
    }


def main(argv):
    if len(argv) >= 3 and argv[0] == "witness":
        result = witness(int(argv[1]), [int(port) for port in argv[2:]])
    elif 4 <= len(argv) <= 6 and argv[0] == "calibrate":
        rate, rtt, port = int(argv[1]), int(argv[2]), int(argv[3])
        mib = int(argv[4]) if len(argv) > 4 else 50
        repeats = int(argv[5]) if len(argv) > 5 else 3
        result = calibrate(rate, rtt, port, mib, repeats)
    else:
        sys.stderr.write(__doc__.split("\n\n")[1] + "\n")
        return 64
    json.dump(result, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
