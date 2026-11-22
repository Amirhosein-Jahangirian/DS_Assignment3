"""Assignment 3: Kubernetes failure experiments 1-7 (soundness proof).

Adapted from the Assignment 2 harness. Each experiment injects a real failure
against the live cluster (minikube, namespace `assignment3`), then verifies
correctness against actual database and transaction state, never only HTTP
responses or pod status.

Usage:
    python k8s_test.py            # run experiments 0-7
    python k8s_test.py 0 1 3      # run a subset

Exit code is 0 only when every selected experiment passes.

Requires: kubectl on PATH, a running cluster with the banking stack deployed,
and Python packages `requests` (only).
"""

import argparse
import atexit
import base64
import concurrent.futures as cf
import decimal
import logging
import socket
import subprocess
import sys
import threading
import time
import uuid

import requests

NAMESPACE = "assignment3"
LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(message)s"
DATE_FORMAT = "%H:%M:%S"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
log = logging.getLogger("k8s_test")

# Accounts used by the suite. A0001-A0003 live on shard-a, A1002/A1003 on shard-b.
ACC_A1 = "A0001"   # deposit target (shard-a)
ACC_A2 = "A0002"   # transfer source (shard-a)
ACC_A3 = "A0003"   # transfer sink (shard-a)
ACC_B2 = "A1002"   # cross-shard sink / shard-b source
ACC_B3 = "A1003"   # shard-b sink

RUN_TAG = uuid.uuid4().hex[:6]


# --------------------------------------------------------------------------
# kubectl helpers
# --------------------------------------------------------------------------

def kubectl(*args, check=True, timeout=90):
    result = subprocess.run(
        ["kubectl", *args], capture_output=True, text=True, timeout=timeout
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def db_password():
    raw = kubectl("get", "secret", "banking-db-credentials", "-n", NAMESPACE,
                  "-o", "jsonpath={.data.password}")
    return base64.b64decode(raw).decode()


def cnpg_primary_pod(cluster):
    """Return the pod acting as primary of a CloudNativePG cluster."""
    out = kubectl("get", "pods", "-n", NAMESPACE,
                  "-l", f"cnpg.io/cluster={cluster}", "-o",
                  "jsonpath={range .items[*]}{.metadata.name} {.metadata.labels.role}{\"\\n\"}{end}")
    for line in out.splitlines():
        name, _, role = line.partition(" ")
        if role == "primary":
            return name
    return None


def psql_on_pod(pod, database, sql):
    """Run one SQL statement on a specific CNPG pod, return stdout."""
    out = kubectl("exec", pod, "-n", NAMESPACE, "--",
                  "env", f"PGPASSWORD={_PGPASS}",
                  "psql", "-h", "127.0.0.1", "-U", "banker", "-d", database,
                  "-t", "-A", "-c", sql)
    return out.strip()


def psql(cluster, database, sql):
    """Run one SQL statement on the primary of a CNPG cluster, return stdout."""
    pod = cnpg_primary_pod(cluster)
    if pod is None:
        raise RuntimeError(f"no primary pod found for CNPG cluster {cluster}")
    return psql_on_pod(pod, database, sql)


def balance(cluster, database, account):
    val = psql(cluster, database,
               f"SELECT balance FROM transactions_account "
               f"WHERE account_number='{account}'")
    return decimal.Decimal(val)


def balance_on_pod(pod, database, account):
    """Read a balance directly from one pod (also works on a hot standby)."""
    val = psql_on_pod(pod, database,
                      f"SELECT balance FROM transactions_account "
                      f"WHERE account_number='{account}'")
    return decimal.Decimal(val)


def shard_total(cluster, database):
    val = psql(cluster, database, "SELECT sum(balance) FROM transactions_account")
    return decimal.Decimal(val or "0")


def ready_pod(app_label):
    """Return the name of a Ready pod for the given app label."""
    out = kubectl(
        "get", "pods", "-n", NAMESPACE, "-l", f"app={app_label}", "-o",
        "jsonpath={range .items[*]}{.metadata.name} "
        "{.status.containerStatuses[0].ready}{\"\\n\"}{end}",
    )
    for line in out.splitlines():
        name, _, ready = line.partition(" ")
        if ready == "true":
            return name
    raise RuntimeError(f"no Ready pod found for app={app_label}")


def seed_accounts(balance_value="1000.00"):
    """Reset both shards so every account starts from the same balance.

    Each shard owns its own account range, so the management command runs once
    per shard, through a Ready API pod that already has the DB env and config.
    """
    for shard in ("shard-a", "shard-b"):
        pod = ready_pod(shard)
        out = kubectl("exec", pod, "-n", NAMESPACE, "--",
                      "python", "manage.py", "seed_accounts",
                      "--reset", "--balance", balance_value)
        last = out.strip().splitlines()[-1] if out.strip() else "no output"
        log.info("seed %s via %s: %s", shard, pod, last)
    log.info("seeded both shards at %s per account", balance_value)


def coordinator_rows(prefix):
    sql = ("SELECT request_id, state, decision FROM coordinator_transactions "
           f"WHERE request_id LIKE '{prefix}%' ORDER BY request_id")
    rows = []
    for line in psql("coord-db", "banking_coord", sql).splitlines():
        if line.strip():
            rows.append(line.split("|"))
    return [tuple(r.strip() for r in row) for row in rows]


# --------------------------------------------------------------------------
# gateway access (NodePort is not reachable from the host; use port-forward)
# --------------------------------------------------------------------------

class PortForward:
    def __init__(self):
        self.proc = None
        self.port = None
        self.lock = threading.Lock()

    def start(self):
        with self.lock:
            for attempt in range(4):
                self.stop_locked()
                self.port = _free_port()
                self.proc = subprocess.Popen(
                    ["kubectl", "port-forward", "svc/gateway",
                     f"{self.port}:8000", "-n", NAMESPACE],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )
                base = f"http://127.0.0.1:{self.port}"
                deadline = time.time() + 90  # gateway startup probe alone can take 90s
                while time.time() < deadline:
                    try:
                        response = requests.get(f"{base}/health", timeout=5)
                        if response.ok:
                            log.info("gateway reachable via port-forward on port %s", self.port)
                            return base
                    except requests.RequestException:
                        pass
                    time.sleep(2)
                log.warning("port-forward attempt %d/4 failed; retrying", attempt + 1)
            raise RuntimeError("port-forward to gateway did not become ready")

    def ensure(self):
        """Return a working base URL, restarting the tunnel if it died."""
        if self.port is not None and self.proc is not None and self.proc.poll() is None:
            try:
                response = requests.get(f"http://127.0.0.1:{self.port}/health", timeout=5)
                if response.ok:
                    return f"http://127.0.0.1:{self.port}"
            except requests.RequestException:
                pass
        return self.start()

    def stop_locked(self):
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            self.proc = None

    def stop(self):
        with self.lock:
            self.stop_locked()


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


PF = PortForward()
atexit.register(PF.stop)


def api(method, path, body=None, timeout=60, session=None):
    base = f"http://127.0.0.1:{PF.port}"
    fn = (session or requests).request
    return fn(method, f"{base}{path}", json=body, timeout=timeout)


def gateway_health(timeout=30):
    return api("GET", "/health", timeout=timeout).json()


def wait_gateway_healthy(timeout=180):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            health = gateway_health(timeout=15)
            shards = health.get("shards", {})
            all_healthy = all(s.get("healthy") for s in shards.values())
            if all_healthy:
                return health
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
        time.sleep(3)
    raise RuntimeError(f"gateway did not become healthy: {last_error}")


def post_until_answered(session, path, payload, deadline):
    """POST one idempotent request until the server gives a definitive answer.

    A timeout or a 5xx (for example OUTCOME_UNKNOWN) means the effect may
    already be applied. Resending the same request_id is safe: the server
    applies it once and replays the stored response, so the caller can count
    exactly the requests that were acknowledged.
    """
    base = f"http://127.0.0.1:{PF.port}"
    response = None
    while True:
        try:
            response = session.post(f"{base}{path}", json=payload, timeout=10)
        except requests.RequestException:
            response = None
        if response is not None and response.status_code < 500:
            return response
        if time.time() >= deadline:
            return response
        time.sleep(0.2)


def wait_transfer(txid, timeout=120):
    """Poll a cross-shard transaction until it reaches a terminal state."""
    deadline = time.time() + timeout
    body = {}
    while time.time() < deadline:
        try:
            body = api("GET", f"/api/transactions/{txid}", timeout=15).json()
        except (requests.RequestException, ValueError):
            body = {}
        if body.get("state") in {"COMPLETED", "ABORTED", "ABORT_DECIDED"}:
            return body
        time.sleep(1)
    return body


# --------------------------------------------------------------------------
# experiment framework
# --------------------------------------------------------------------------

class Result:
    def __init__(self, number, title):
        self.number = number
        self.title = title
        self.checks = []
        self.notes = []

    def check(self, name, condition, detail=""):
        self.checks.append((name, bool(condition), detail))
        mark = "PASS" if condition else "FAIL"
        log.info("  [%s] %s%s", mark, name, f" - {detail}" if detail else "")
        return condition

    def note(self, text):
        self.notes.append(text)
        log.info("  [NOTE] %s", text)

    @property
    def ok(self):
        return all(passed for _, passed, _ in self.checks)


def begin(number, title):
    line = "=" * 74
    log.info("%s", line)
    log.info("EXPERIMENT %s: %s", number, title)
    log.info("%s", line)
    return Result(number, title)


# --------------------------------------------------------------------------
# experiment 0: normal banking operations
# --------------------------------------------------------------------------

def experiment_0(result):
    """One of each normal operation, checked against database state."""
    tag = f"exp0-{RUN_TAG}"

    # --- read -------------------------------------------------------------
    expected = balance("db-a", "banking_a", ACC_A1)
    body = api("GET", f"/api/accounts/{ACC_A1}/", timeout=30).json()
    served = decimal.Decimal(body.get("balance") or "0")
    result.check("account read matches the database",
                 served == expected,
                 f"{ACC_A1} api={served} db={expected}")

    # --- deposit ----------------------------------------------------------
    r = api("POST", f"/api/accounts/{ACC_A1}/deposit/",
            {"amount": 50, "request_id": f"{tag}-dep"}, timeout=60)
    after_deposit = balance("db-a", "banking_a", ACC_A1)
    result.check("deposit accepted",
                 r.ok and (r.json() or {}).get("status") == "success",
                 f"HTTP {r.status_code} {r.text[:120]}")
    result.check("deposit applied exactly once",
                 after_deposit - expected == decimal.Decimal("50.00"),
                 f"delta={after_deposit - expected} expected=50.00")

    # --- withdraw ---------------------------------------------------------
    r = api("POST", f"/api/accounts/{ACC_A1}/withdraw/",
            {"amount": 20, "request_id": f"{tag}-wdr"}, timeout=60)
    after_withdraw = balance("db-a", "banking_a", ACC_A1)
    result.check("withdraw accepted",
                 r.ok and (r.json() or {}).get("status") == "success",
                 f"HTTP {r.status_code} {r.text[:120]}")
    result.check("withdraw applied exactly once",
                 after_deposit - after_withdraw == decimal.Decimal("20.00"),
                 f"delta={after_deposit - after_withdraw} expected=20.00")

    # --- same-shard transfer ---------------------------------------------
    # A previous run may have drained the source account; fund it so the
    # smoke-test transfer cannot fail for insufficient funds.
    if balance("db-a", "banking_a", ACC_A2) < decimal.Decimal("50.00"):
        api("POST", f"/api/accounts/{ACC_A2}/deposit/",
            {"amount": 100, "request_id": f"{tag}-topup"}, timeout=60)

    src_before = balance("db-a", "banking_a", ACC_A2)
    dst_before = balance("db-a", "banking_a", ACC_A3)
    r = api("POST", "/api/transfers/",
            {"from_account": ACC_A2, "to_account": ACC_A3,
             "amount": 10, "request_id": f"{tag}-local"}, timeout=120)
    src_after = balance("db-a", "banking_a", ACC_A2)
    dst_after = balance("db-a", "banking_a", ACC_A3)
    result.check("same-shard transfer accepted",
                 r.ok and (r.json() or {}).get("status") == "success",
                 f"HTTP {r.status_code} {r.text[:120]}")
    result.check("same-shard transfer moved 10.00",
                 src_before - src_after == decimal.Decimal("10.00")
                 and dst_after - dst_before == decimal.Decimal("10.00"),
                 f"source {src_before}->{src_after} sink {dst_before}->{dst_after}")

    # --- cross-shard transfer ---------------------------------------------
    src_before = balance("db-a", "banking_a", ACC_A2)
    dst_before = balance("db-b", "banking_b", ACC_B2)
    r = api("POST", "/api/transfers/",
            {"from_account": ACC_A2, "to_account": ACC_B2,
             "amount": 10, "request_id": f"{tag}-cross"}, timeout=120)
    body = r.json() or {}
    txid = body.get("txid")
    result.check("cross-shard transfer accepted",
                 r.status_code in (200, 202) and bool(txid),
                 f"HTTP {r.status_code} txid={txid}")
    final = wait_transfer(txid) if txid else {}
    result.check("cross-shard transfer completed",
                 final.get("state") == "COMPLETED",
                 f"state={final.get('state')} decision={final.get('decision')}")
    src_after = balance("db-a", "banking_a", ACC_A2)
    dst_after = balance("db-b", "banking_b", ACC_B2)
    result.check("cross-shard transfer moved 10.00",
                 src_before - src_after == decimal.Decimal("10.00")
                 and dst_after - dst_before == decimal.Decimal("10.00"),
                 f"source {src_before}->{src_after} sink {dst_before}->{dst_after}")


# --------------------------------------------------------------------------
# experiment 1: application pod failure
# --------------------------------------------------------------------------

def experiment_1(result):
    cluster, database = "db-a", "banking_a"
    before = balance(cluster, database, "A0100")
    log.info("baseline: pod list and balance A0100=%s", before)

    log.info("injecting failure: force-delete pod shard-a-1 (grace period 0)")
    kubectl("delete", "pod", "shard-a-1", "-n", NAMESPACE,
            "--force", "--grace-period=0")

    ok = fail = 0
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            body = api("GET", f"/api/accounts/A0100/", timeout=8).json()
            if body.get("balance"):
                ok += 1
            else:
                fail += 1
        except requests.RequestException:
            fail += 1
        time.sleep(0.25)
    log.info("failure window: %d served, %d failed", ok, fail)
    result.check("healthy replica kept serving during pod failure", ok >= 30 and fail == 0,
                 f"served={ok} failed={fail}")

    deadline = time.time() + 180
    healed = False
    while time.time() < deadline:
        pods = kubectl("get", "pods", "-n", NAMESPACE, "-l", "app=shard-a",
                       "--field-selector", "status.phase=Running")
        names = [line.split()[0] for line in pods.splitlines()[1:]]
        ready = [n for n in names if kubectl(
            "get", "pod", n, "-n", NAMESPACE, "-o",
            "jsonpath={.status.containerStatuses[0].ready}") == "true"]
        if len(ready) >= 2:
            healed = True
            break
        time.sleep(5)
    log.info("pod recovery: ready replicas=%s", ready if healed else "timeout")
    result.check("killed pod replaced automatically", healed)

    health = wait_gateway_healthy()
    healthy = health["shards"]["shard-a"]["healthy"]
    result.check("both shard-a replicas healthy again", len(healthy) >= 2, str(healthy))

    after = balance(cluster, database, "A0100")
    result.check("balance unchanged by the failure", before == after,
                 f"before={before} after={after}")


# --------------------------------------------------------------------------
# experiment 2: primary database failure
# --------------------------------------------------------------------------

def experiment_2(result):
    cluster, database = "db-a", "banking_a"
    primary_before = cnpg_primary_pod(cluster)
    before = balance(cluster, database, "A0100")
    total_before = shard_total(cluster, database)
    log.info("baseline: primary=%s A0100=%s shard_total=%s",
             primary_before, before, total_before)

    log.info("injecting failure: force-delete primary DB pod %s", primary_before)
    kubectl("delete", "pod", primary_before, "-n", NAMESPACE,
            "--force", "--grace-period=0")

    log.info("waiting for CloudNativePG to promote the standby (up to 5 min)")
    new_primary = None
    deadline = time.time() + 300
    while time.time() < deadline:
        new_primary = cnpg_primary_pod(cluster)
        if new_primary and new_primary != primary_before:
            break
        time.sleep(5)
    result.check("standby promoted to primary",
                 bool(new_primary and new_primary != primary_before),
                 f"new primary={new_primary}")

    after = balance(cluster, database, "A0100")
    total_after = shard_total(cluster, database)
    result.check("committed balances preserved after failover", before == after,
                 f"A0100 before={before} after={after}")
    result.check("shard total conserved", total_before == total_after,
                 f"{total_before} -> {total_after}")

    # The shard APIs reconnect to the promoted primary after failover; until
    # their next health probe the gateway may answer NO_HEALTHY_REPLICA for a
    # few seconds. Allow that short window instead of judging one probe.
    body = {}
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            body = api("GET", "/api/accounts/A0100/", timeout=30).json()
        except (requests.RequestException, ValueError):
            body = {}
        if body.get("balance") is not None:
            break
        time.sleep(3)
    result.check("API serves reads against the new primary",
                 body.get("balance") is not None, str(body))

    # Write after the promotion, then confirm the former primary picks the
    # write up once it rejoins as a streaming replica.
    write_amount = decimal.Decimal("7.00")
    before_write = balance(cluster, database, "A0100")
    r = api("POST", "/api/accounts/A0100/deposit/",
            {"amount": str(write_amount), "request_id": f"exp2-{RUN_TAG}-catchup"},
            timeout=60)
    result.check("post-failover deposit accepted", r.ok, f"HTTP {r.status_code}")
    after_write = balance(cluster, database, "A0100")
    result.check("post-failover deposit applied on the new primary",
                 after_write - before_write == write_amount,
                 f"{before_write} -> {after_write}")

    replica_value = None
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            replica_value = balance_on_pod(primary_before, database, "A0100")
        except RuntimeError:
            replica_value = None
        if replica_value == after_write:
            break
        time.sleep(5)
    result.check("recovered former primary replicates the new write",
                 replica_value == after_write,
                 f"{primary_before} balance={replica_value} expected={after_write}")

    log.info("rejoined old pod: %s", kubectl(
        "get", "pods", "-n", NAMESPACE, "-l", f"cnpg.io/cluster={cluster}", "-L", "role"))


# --------------------------------------------------------------------------
# experiment 3: concurrent transactions
# --------------------------------------------------------------------------

def experiment_3(result):
    cluster, database = "db-a", "banking_a"
    b_a1, b_a2, b_a3 = (balance(cluster, database, a) for a in (ACC_A1, ACC_A2, ACC_A3))
    total_before = shard_total(cluster, database)
    log.info("baseline: %s=%s %s=%s %s=%s total=%s",
             ACC_A1, b_a1, ACC_A2, b_a2, ACC_A3, b_a3, total_before)

    tag = f"exp3-{RUN_TAG}"

    def deposit(i):
        r = api("POST", f"/api/accounts/{ACC_A1}/deposit/",
                {"amount": 5, "request_id": f"{tag}-dep-{i:03d}"}, timeout=60)
        return r.ok

    def transfer(i):
        r = api("POST", "/api/transfers/",
                {"from_account": ACC_A2, "to_account": ACC_A3,
                 "amount": 10, "request_id": f"{tag}-xf-{i:03d}"}, timeout=120)
        return r.ok

    log.info("firing 20 concurrent deposits (+5) and 10 concurrent transfers (10) in parallel")
    with cf.ThreadPoolExecutor(30) as pool:
        deposits = list(pool.map(deposit, range(20)))
        transfers = list(pool.map(transfer, range(10)))
    log.info("http results: deposits ok=%d/20, transfers ok=%d/10",
             sum(deposits), sum(transfers))
    result.check("all concurrent requests acknowledged", all(deposits) and all(transfers),
                 f"deposits={sum(deposits)}/20 transfers={sum(transfers)}/10")

    a_a1 = balance(cluster, database, ACC_A1)
    a_a2 = balance(cluster, database, ACC_A2)
    a_a3 = balance(cluster, database, ACC_A3)
    result.check("20 parallel deposits applied exactly once",
                 a_a1 - b_a1 == decimal.Decimal("100.00"),
                 f"delta={a_a1 - b_a1} expected=100.00")
    result.check("10 parallel transfers debited source exactly once",
                 b_a2 - a_a2 == decimal.Decimal("100.00"),
                 f"delta={b_a2 - a_a2} expected=100.00")
    result.check("10 parallel transfers credited sink exactly once",
                 a_a3 - b_a3 == decimal.Decimal("100.00"),
                 f"delta={a_a3 - b_a3} expected=100.00")
    total_after = shard_total(cluster, database)
    result.check("money conserved under concurrency", total_after - total_before == decimal.Decimal("100.00"),
                 f"total delta={total_after - total_before} expected=100.00")


# --------------------------------------------------------------------------
# experiment 4: coordinator interruption
# --------------------------------------------------------------------------

def experiment_4(result):
    before_a = balance("db-a", "banking_a", ACC_A2)
    before_b = balance("db-b", "banking_b", ACC_B2)
    log.info("baseline: %s=%s %s=%s", ACC_A2, before_a, ACC_B2, before_b)

    tag = f"exp4-{RUN_TAG}"

    def burst():
        def one(i):
            try:
                r = api("POST", "/api/transfers/",
                        {"from_account": ACC_A2, "to_account": ACC_B2,
                         "amount": 1, "request_id": f"{tag}-{i:03d}"}, timeout=25)
                return (i, r.status_code)
            except requests.RequestException:
                return (i, "CLIENT_ERROR")
        with cf.ThreadPoolExecutor(20) as pool:
            return list(pool.map(one, range(20)))

    outcomes = []
    killer = threading.Timer(1.2, lambda: kubectl(
        "delete", "pod", "-l", "app=gateway", "-n", NAMESPACE,
        "--force", "--grace-period=0"))
    log.info("injecting failure: gateway pod force-deleted 1.2s into a 20-transfer burst")
    killer.start()
    try:
        outcomes = burst()
    finally:
        killer.cancel()

    acked = {i for i, code in outcomes if code == 200}
    unknown = {i for i, code in outcomes if code == "CLIENT_ERROR"}
    log.info("client view: %d acknowledged (HTTP 200), %d unknown outcome (connection killed)",
             len(acked), len(unknown))

    log.info("restarting gateway tunnel and waiting for the recovery loop")
    PF.start()
    wait_gateway_healthy()

    deadline = time.time() + 150
    rows = []
    while time.time() < deadline:
        rows = coordinator_rows(tag)
        if rows and all(state in ("COMPLETED", "ABORTED") for _, state, _ in rows):
            break
        time.sleep(5)
    states = {rid: (state, decision) for rid, state, decision in rows}
    result.check("recovery loop resolved every interrupted transaction",
                 len(rows) == 20 and all(s in ("COMPLETED", "ABORTED") for _, s, _ in rows),
                 f"rows={len(rows)} states={sorted(set(s for _, s, _ in rows))}")

    commits = [rid for rid, (state, decision) in states.items() if decision == "COMMIT"]
    aborts = [rid for rid, (state, decision) in states.items() if decision == "ABORT"]
    log.info("outcomes: %d COMMIT, %d ABORT", len(commits), len(aborts))
    result.check("no unresolved/partial transactions", len(commits) + len(aborts) == len(rows))

    acked_committed = all(states.get(f"{tag}-{i:03d}", ("", ""))[1] == "COMMIT" for i in acked)
    result.check("every acknowledged success is durable (committed)", acked_committed,
                 f"acked={sorted(acked)}")

    c = decimal.Decimal(len(commits))
    after_a = balance("db-a", "banking_a", ACC_A2)
    after_b = balance("db-b", "banking_b", ACC_B2)
    result.check("debited side matches committed count exactly",
                 before_a - after_a == c, f"delta={before_a - after_a} commits={c}")
    result.check("credited side matches committed count exactly",
                 after_b - before_b == c, f"delta={after_b - before_b} commits={c}")


# --------------------------------------------------------------------------
# experiment 5: autoscaling under load
# --------------------------------------------------------------------------

def _hpa_replicas(name):
    val = kubectl("get", "hpa", name, "-n", NAMESPACE,
                  "-o", "jsonpath={.status.currentReplicas}")
    return int(val or 0)


def experiment_5(result):
    name = "hpa-shard-a"
    base = PF.ensure()
    initial = _hpa_replicas(name)
    if initial > 2:
        log.info("HPA above min (%d replicas); waiting for scale-down before the test", initial)
        deadline = time.time() + 420
        while time.time() < deadline and _hpa_replicas(name) > 2:
            time.sleep(15)
        initial = _hpa_replicas(name)
        if initial > 2:
            result.note(f"HPA still at {initial} replicas after 7 min; continuing anyway")

    before_a = balance("db-a", "banking_a", ACC_A2)
    before_b = balance("db-a", "banking_a", ACC_A3)
    log.info("baseline: replicas=%d %s=%s %s=%s", initial, ACC_A2, before_a, ACC_A3, before_b)

    tag = f"exp5-{RUN_TAG}"
    ok_count = 0
    lock = threading.Lock()

    def worker(n):
        nonlocal ok_count
        session = requests.Session()
        i = 0
        stop = time.time() + 180
        while time.time() < stop:
            r = post_until_answered(
                session, "/api/transfers/",
                {"from_account": ACC_A2, "to_account": ACC_A3,
                 "amount": 0.01, "request_id": f"{tag}-{n}-{i:05d}"},
                stop + 20)
            if r is not None and r.ok:
                with lock:
                    ok_count += 1
            i += 1

    log.info("starting 8-thread load for 3 minutes (same-shard transfers of 0.01)")
    threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()

    peak = initial
    scaled = False
    while any(t.is_alive() for t in threads):
        current = _hpa_replicas(name)
        if current > peak:
            log.info("autoscaler reaction: replicas %d -> %d", peak, current)
            peak = current
            if current > initial:
                scaled = True
        time.sleep(10)
    for t in threads:
        t.join()

    log.info("load finished: ok=%d, replicas went %d -> %d", ok_count, initial, peak)
    if initial >= 5:
        result.note("HPA already at max before load; scale-up could not be exercised")
    else:
        result.check("workload-driven scale-up occurred", scaled,
                     f"replicas {initial} -> {peak}")

    expected = decimal.Decimal("0.01") * ok_count
    after_a = balance("db-a", "banking_a", ACC_A2)
    after_b = balance("db-a", "banking_a", ACC_A3)
    result.check("every acknowledged transfer applied exactly once while scaling",
                 before_a - after_a == expected,
                 f"debit delta={before_a - after_a} expected={expected}")
    result.check("credit side exact while scaling",
                 after_b - before_b == expected,
                 f"credit delta={after_b - before_b} expected={expected}")


# --------------------------------------------------------------------------
# experiment 6: rolling update under load + failed update rollback
# --------------------------------------------------------------------------

def experiment_6(result):
    base = PF.ensure()
    before_a = balance("db-b", "banking_b", ACC_B2)
    before_b = balance("db-b", "banking_b", ACC_B3)
    log.info("baseline: %s=%s %s=%s", ACC_B2, before_a, ACC_B3, before_b)

    tag = f"exp6-{RUN_TAG}"
    ok_count = 0
    err_count = 0
    lock = threading.Lock()

    def worker(n):
        nonlocal ok_count, err_count
        session = requests.Session()
        i = 0
        stop = time.time() + 150
        while time.time() < stop:
            r = post_until_answered(
                session, "/api/transfers/",
                {"from_account": ACC_B2, "to_account": ACC_B3,
                 "amount": 0.01, "request_id": f"{tag}-{n}-{i:05d}"},
                stop + 20)
            with lock:
                if r is not None and r.ok:
                    ok_count += 1
                else:
                    err_count += 1
            i += 1

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(6)]
    for t in threads:
        t.start()
    time.sleep(10)
    log.info("injecting update: kubectl rollout restart statefulset shard-b at t+10s")
    kubectl("rollout", "restart", "statefulset", "shard-b", "-n", NAMESPACE)
    for t in threads:
        t.join()
    log.info("load finished: ok=%d transient_errors=%d", ok_count, err_count)
    if err_count:
        # Temporary blocking is acceptable per the assignment; correctness is
        # proven by the exact DB deltas below (acked == applied, errors == no effect).
        result.note(f"{err_count} requests temporarily blocked while both shard-b "
                    "replicas restarted (Parallel policy); no false successes issued")

    expected = decimal.Decimal("0.01") * ok_count
    after_a = balance("db-b", "banking_b", ACC_B2)
    after_b = balance("db-b", "banking_b", ACC_B3)
    result.check("rolling update preserved transaction integrity (debit exact)",
                 before_a - after_a == expected,
                 f"debit delta={before_a - after_a} expected={expected}")
    result.check("rolling update preserved transaction integrity (credit exact)",
                 after_b - before_b == expected,
                 f"credit delta={after_b - before_b} expected={expected}")

    # --- failed update and rollback ---------------------------------------
    good_image = kubectl("get", "statefulset", "shard-b", "-n", NAMESPACE,
                         "-o", "jsonpath={.spec.template.spec.containers[0].image}")
    log.info("injecting failed update: image %s -> :does-not-exist", good_image)
    kubectl("set", "image", "statefulset/shard-b",
            f"api=amirhossein002/banking_with_sharding:does-not-exist",
            "-n", NAMESPACE)
    time.sleep(45)

    serving = 0
    for _ in range(15):
        try:
            if api("GET", f"/api/accounts/{ACC_B2}/", timeout=8).ok:
                serving += 1
        except requests.RequestException:
            pass
        time.sleep(1)
    result.check("old replicas kept serving while the update was failing", serving >= 10,
                 f"{serving}/15 reads served during failed rollout")

    log.info("rolling back: kubectl rollout undo statefulset shard-b")
    kubectl("rollout", "undo", "statefulset", "shard-b", "-n", NAMESPACE)
    deadline = time.time() + 240
    rolled = False
    while time.time() < deadline:
        current = kubectl("get", "statefulset", "shard-b", "-n", NAMESPACE,
                          "-o", "jsonpath={.spec.template.spec.containers[0].image}")
        if current == good_image:
            rolled = True
            break
        time.sleep(10)
    result.check("rollback restored the previous image", rolled,
                 f"image={good_image}")

    deadline = time.time() + 180
    while time.time() < deadline:
        status = kubectl("rollout", "status", "statefulset", "shard-b",
                         "-n", NAMESPACE, "--timeout=10s", check=False)
        if "successfully rolled out" in status:
            break
        time.sleep(10)
    body = api("GET", f"/api/accounts/{ACC_B2}/", timeout=30).json()
    result.check("system serving again after rollback", body.get("balance") is not None,
                 str(body))


# --------------------------------------------------------------------------
# experiment 7: repeated client requests (idempotency)
# --------------------------------------------------------------------------

def experiment_7(result):
    """Repeating a request with the same request_id must apply its effect once.

    The expected outcome table requires: repeated client requests produce no
    duplicate transaction effects, proven against DB state.
    """
    tag = f"exp7-{RUN_TAG}"
    repeat = 3

    # --- deposit ----------------------------------------------------------
    before = balance("db-a", "banking_a", ACC_A1)
    for i in range(repeat):
        r = api("POST", f"/api/accounts/{ACC_A1}/deposit/",
                {"amount": 10, "request_id": f"{tag}-dep"}, timeout=60)
        if i == 0:
            result.check("first deposit acknowledged", r.ok, f"HTTP {r.status_code}")
    after = balance("db-a", "banking_a", ACC_A1)
    result.check(f"deposit repeated {repeat}x with one request_id applied once",
                 after - before == decimal.Decimal("10.00"),
                 f"delta={after - before} expected=10.00")

    # --- withdraw ---------------------------------------------------------
    before = balance("db-a", "banking_a", ACC_A1)
    for i in range(repeat):
        r = api("POST", f"/api/accounts/{ACC_A1}/withdraw/",
                {"amount": 5, "request_id": f"{tag}-wdr"}, timeout=60)
    after = balance("db-a", "banking_a", ACC_A1)
    result.check(f"withdraw repeated {repeat}x with one request_id applied once",
                 before - after == decimal.Decimal("5.00"),
                 f"delta={before - after} expected=5.00")

    # --- same-shard transfer ---------------------------------------------
    # Earlier load experiments can leave the source account short; fund it
    # first so a 10.00 transfer is never rejected for insufficient funds.
    current = balance("db-a", "banking_a", ACC_A2)
    if current < decimal.Decimal("50.00"):
        r = api("POST", f"/api/accounts/{ACC_A2}/deposit/",
                {"amount": 100, "request_id": f"{tag}-topup"}, timeout=60)
        result.check("source account funded for transfer checks", r.ok,
                     f"HTTP {r.status_code} balance was {current}")

    before_a = balance("db-a", "banking_a", ACC_A2)
    before_b = balance("db-a", "banking_a", ACC_A3)
    for i in range(repeat):
        r = api("POST", "/api/transfers/",
                {"from_account": ACC_A2, "to_account": ACC_A3,
                 "amount": 10, "request_id": f"{tag}-xf"}, timeout=120)
        if i == 0:
            result.check("first same-shard transfer acknowledged", r.ok,
                         f"HTTP {r.status_code}")
    after_a = balance("db-a", "banking_a", ACC_A2)
    after_b = balance("db-a", "banking_a", ACC_A3)
    result.check(f"same-shard transfer repeated {repeat}x applied once (debit)",
                 before_a - after_a == decimal.Decimal("10.00"),
                 f"delta={before_a - after_a} expected=10.00")
    result.check(f"same-shard transfer repeated {repeat}x applied once (credit)",
                 after_b - before_b == decimal.Decimal("10.00"),
                 f"delta={after_b - before_b} expected=10.00")

    # --- cross-shard transfer ---------------------------------------------
    before_a = balance("db-a", "banking_a", ACC_A2)
    before_b = balance("db-b", "banking_b", ACC_B2)
    first_txid = None
    same_txid = True
    for i in range(repeat):
        r = api("POST", "/api/transfers/",
                {"from_account": ACC_A2, "to_account": ACC_B2,
                 "amount": 10, "request_id": f"{tag}-xs"}, timeout=120)
        body = r.json() or {}
        if i == 0:
            result.check("first cross-shard transfer acknowledged", r.ok,
                         f"HTTP {r.status_code}")
            first_txid = body.get("txid")
        elif body.get("txid") != first_txid:
            same_txid = False
    result.check("cross-shard replay returned the same txid", same_txid,
                 f"txid={first_txid}")
    after_a = balance("db-a", "banking_a", ACC_A2)
    after_b = balance("db-b", "banking_b", ACC_B2)
    result.check(f"cross-shard transfer repeated {repeat}x applied once (debit)",
                 before_a - after_a == decimal.Decimal("10.00"),
                 f"delta={before_a - after_a} expected=10.00")
    result.check(f"cross-shard transfer repeated {repeat}x applied once (credit)",
                 after_b - before_b == decimal.Decimal("10.00"),
                 f"delta={after_b - before_b} expected=10.00")
    rows = coordinator_rows(f"{tag}-xs")
    result.check("coordinator stored exactly one row for the replayed request",
                 len(rows) == 1, f"rows={len(rows)}")


EXPERIMENTS = {
    0: ("Normal Banking Operations", experiment_0),
    1: ("Application Pod Failure", experiment_1),
    2: ("Primary Database Failure", experiment_2),
    3: ("Concurrent Transactions", experiment_3),
    4: ("Coordinator Interruption", experiment_4),
    5: ("Autoscaling Under Load", experiment_5),
    6: ("Rolling Update Under Load + Rollback", experiment_6),
    7: ("Repeated Client Requests (Idempotency)", experiment_7),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("numbers", nargs="*", type=int,
                        help="experiments to run (default: all 0-7)")
    args = parser.parse_args()
    selected = args.numbers or list(EXPERIMENTS)

    log.info("verifying cluster is reachable")
    kubectl("get", "ns", NAMESPACE)
    PF.start()
    wait_gateway_healthy()
    global _PGPASS
    _PGPASS = db_password()

    results = []

    def run(number):
        title, fn = EXPERIMENTS[number]
        result = begin(number, title)
        started = time.time()
        try:
            fn(result)
        except Exception as exc:  # keep the suite running across failures
            log.exception("experiment %s aborted: %s", number, exc)
            result.check("experiment completed without errors", False, repr(exc))
        result.elapsed = time.time() - started
        return result

    if 0 in selected:
        results.append(run(0))
        selected = [n for n in selected if n != 0]

    # Experiment 1 and the rest start from a known balance: every account at
    # 1000.00 on both shards.
    seed_accounts()
    for number in selected:
        results.append(run(number))

    log.info("%s", "=" * 74)
    log.info("SUMMARY")
    log.info("%s", "=" * 74)
    for result in results:
        log.info("  %s  %-42s  %6.1fs", "PASS" if result.ok else "FAIL",
                 result.title, result.elapsed)
        for name, passed, detail in result.checks:
            if not passed:
                log.info("         FAILED CHECK: %s %s", name, detail)
    passed = sum(1 for r in results if r.ok)
    log.info("Experiments: %d/%d passed", passed, len(results))
    sys.exit(0 if passed == len(results) else 1)


if __name__ == "__main__":
    main()
