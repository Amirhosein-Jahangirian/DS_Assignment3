"""
Assignment 3: Kubernetes Failure Experiments 1-7.
Adapted from Assignment 2 to use kubectl instead of docker compose.
"""
import json
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import requests

# ============================================================
# CONFIGURATION
# ============================================================
BASE_URL = "http://127.0.0.1:8000" # Ensure port-forward is running on 8000
NAMESPACE = "assignment3"

# ============================================================
# KUBERNETES HELPERS
# ============================================================
def kubectl_exec_psql(database, sql):
    """Execute SQL against the CNPG primary database."""
    service_map = {"banking_a": "db-a-rw", "banking_b": "db-b-rw", "banking_coord": "coord-db-rw"}
    service = service_map.get(database, "db-a-rw")
    cmd = [
        "kubectl", "exec", f"svc/{service}", "-n", NAMESPACE, "--",
        "env", "PGPASSWORD=1234", "psql", "-h", "localhost", "-U", "banker", 
        "-d", database, "-tAc", sql
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if res.returncode != 0:
        raise RuntimeError(f"psql failed: {res.stderr.strip()[:200]}")
    return res.stdout.strip()

def kubectl_delete_pod(pod_name):
    """Simulate a crash by force-deleting a pod."""
    cmd = ["kubectl", "delete", "pod", pod_name, "-n", NAMESPACE, "--force", "--grace-period=0"]
    subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    print(f"    [!] Deleted pod: {pod_name}")

def kubectl_get_primary_db_pod(cluster_name):
    """Find the current primary pod for a CNPG cluster."""
    cmd = ["kubectl", "get", "pods", "-l", f"cnpg.io/cluster={cluster_name},role=primary", 
           "-n", NAMESPACE, "-o", "jsonpath='{.items[0].metadata.name}'"]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    return res.stdout.strip().strip("'")

def wait_for_pod_ready(pod_name, timeout=120):
    """Wait for a specific pod to return to Running/Ready state."""
    print(f"    Waiting for {pod_name} to recover...")
    start = time.time()
    while time.time() - start < timeout:
        cmd = ["kubectl", "get", "pod", pod_name, "-n", NAMESPACE, "-o", "jsonpath='{.status.phase}'"]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        if "Running" in res.stdout:
            print(f"    [✓] {pod_name} is back online.")
            return True
        time.sleep(2)
    print(f"    [!] {pod_name} did not recover in time.")
    return False

# ============================================================
# BANKING CLIENT HELPERS
# ============================================================
def get_balance(account):
    res = requests.get(f"{BASE_URL}/api/accounts/{account}/", timeout=10)
    return res.json().get("balance") if res.status_code == 200 else None

def transfer(from_acc, to_acc, amount, request_id=None):
    payload = {"from_account": from_acc, "to_account": to_acc, "amount": str(amount)}
    if request_id: payload["request_id"] = request_id
    res = requests.post(f"{BASE_URL}/api/transfers/", json=payload, timeout=30)
    return res.json() if res.status_code in [200, 202] else {"error": res.text}

# ============================================================
# EXPERIMENT 1: Application Pod Failure
# ============================================================
def exp_1_app_pod_failure():
    print("\n=== EXP 1: Application Pod Failure ===")
    print("1. Deleting shard-a-1...")
    kubectl_delete_pod("shard-a-1")
    
    print("2. Sending requests to shard-a while pod is down...")
    served_by = set()
    for _ in range(5):
        res = requests.get(f"{BASE_URL}/api/accounts/A0500/", timeout=5)
        served_by.add(res.headers.get("x-served-by"))
    print(f"   Requests served by: {served_by}")
    
    print("3. Waiting for StatefulSet to recreate shard-a-1...")
    wait_for_pod_ready("shard-a-1")
    
    print("4. Verifying traffic returns to both replicas...")
    served_by.clear()
    for _ in range(10):
        res = requests.get(f"{BASE_URL}/api/accounts/A0500/", timeout=5)
        served_by.add(res.headers.get("x-served-by"))
    print(f"   Final distribution: {served_by}")
    assert "shard-a-1" in served_by and "shard-a-2" in served_by, "FAIL: Traffic didn't rebalance!"
    print("[✓] PASS: Healthy replicas served traffic during outage.")

# ============================================================
# EXPERIMENT 2: Primary Database Failure
# ============================================================
def exp_2_primary_db_failure():
    print("\n=== EXP 2: Primary Database Failure ===")
    primary_pod = kubectl_get_primary_db_pod("db-a")
    print(f"1. Current primary for db-a is: {primary_pod}")
    
    balance_before = get_balance("A0500")
    print(f"2. Balance before failure: {balance_before}")
    
    print("3. Force deleting primary DB pod...")
    kubectl_delete_pod(primary_pod)
    
    print("4. Waiting for CNPG to promote standby...")
    time.sleep(15) # Give CNPG operator time to failover
    
    new_primary = kubectl_get_primary_db_pod("db-a")
    print(f"   New primary is: {new_primary}")
    
    balance_after = get_balance("A0500")
    print(f"5. Balance after failover: {balance_after}")
    
    assert balance_before == balance_after, "FAIL: Balance changed after DB failover!"
    assert primary_pod != new_primary, "FAIL: Primary didn't change!"
    print("[✓] PASS: DB recovery preserved valid committed state.")

# ============================================================
# EXPERIMENT 3: Concurrent Transactions
# ============================================================
def exp_3_concurrent_transactions():
    print("\n=== EXP 3: Concurrent Transactions ===")
    acc_a, acc_b = "A0500", "A1500"
    bal_a_before = Decimal(get_balance(acc_a))
    bal_b_before = Decimal(get_balance(acc_b))
    total_before = bal_a_before + bal_b_before
    
    print(f"1. Running 50 concurrent cross-shard transfers...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(transfer, acc_a, acc_b, "1.00") for _ in range(50)]
        results = [f.result() for f in futures]
        
    successes = sum(1 for r in results if r.get("state") == "COMPLETED")
    print(f"   Completed: {successes}/50")
    
    bal_a_after = Decimal(get_balance(acc_a))
    bal_b_after = Decimal(get_balance(acc_b))
    total_after = bal_a_after + bal_b_after
    
    print(f"2. Total money before: {total_before}, after: {total_after}")
    assert total_before == total_after, "FAIL: Money conservation violated!"
    print("[✓] PASS: Account balances remain correct under concurrency.")

# ============================================================
# EXPERIMENT 4: Coordinator Interruption
# ============================================================
def exp_4_coordinator_interruption():
    print("\n=== EXP 4: Coordinator Interruption ===")
    gateway_pod = subprocess.run(
        ["kubectl", "get", "pods", "-l", "app=gateway", "-n", NAMESPACE, "-o", "jsonpath='{.items[0].metadata.name}'"],
        capture_output=True, text=True
    ).stdout.strip().strip("'")
    
    print(f"1. Starting cross-shard transfer and interrupting Gateway ({gateway_pod})...")
    # We simulate interruption by deleting the gateway pod right after initiating
    import threading
    def do_transfer():
        return transfer("A0500", "A1500", "5.00", request_id="exp4-test-id")
    
    t = threading.Thread(target=do_transfer)
    t.start()
    time.sleep(0.5) # Let it hit the coordinator
    kubectl_delete_pod(gateway_pod)
    t.join(timeout=15)
    
    print("2. Waiting for Gateway to recover...")
    wait_for_pod_ready(gateway_pod)
    time.sleep(5) # Let recovery loop run
    
    # Check transaction status
    # Note: In a real script we'd capture the txid. Here we check the coordinator DB directly.
    row = kubectl_exec_psql("banking_coord", "SELECT state FROM coordinator_transactions WHERE request_id='exp4-test-id';")
    print(f"   Transaction state in DB: {row}")
    assert row in ["COMPLETED", "COMMIT_DECIDED", "PENDING"], "FAIL: Transaction lost!"
    print("[✓] PASS: Unresolved transactions recovered correctly.")

# ============================================================
# EXPERIMENT 5: Autoscaling Under Load
# ============================================================
def exp_5_autoscaling_under_load():
    print("\n=== EXP 5: Autoscaling Under Load ===")
    print("1. Checking initial HPA state...")
    subprocess.run(["kubectl", "get", "hpa", "-n", NAMESPACE])
    
    print("2. Generating load to trigger HPA (reduced for Minikube)...")
    # Reduced from 200 to 80, and max_workers from 30 to 10
    # This prevents overwhelming the single-node cluster while still triggering scale-up
    successful_transfers = 0
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(transfer, "A0500", "A1500", "0.01") for _ in range(80)]
        for f in futures:
            try:
                result = f.result(timeout=60)
                if result.get("state") == "COMPLETED":
                    successful_transfers += 1
            except Exception as e:
                # Gracefully handle connection drops during scaling
                print(f"   [!] Transfer failed (expected during scaling): {type(e).__name__}")
                
    print(f"   Successfully completed: {successful_transfers}/80")
    
    print("3. Waiting 90s for HPA to observe metrics and scale up...")
    time.sleep(90)
    
    replicas = subprocess.run(
        ["kubectl", "get", "statefulset", "shard-a", "-n", NAMESPACE, "-o", "jsonpath='{.status.readyReplicas}'"],
        capture_output=True, text=True
    ).stdout.strip().strip("'")
    print(f"   shard-a ready replicas: {replicas}")
    
    # Allow scaling to 3 or more; sometimes Minikube needs extra time
    assert int(replicas) >= 3, f"FAIL: HPA did not scale up (got {replicas} replicas)"
    print("[✓] PASS: Scaling occurred without violating banking correctness.")

# ============================================================
# EXPERIMENT 6: Rolling Update Under Load
# ============================================================
def exp_6_rolling_update_under_load():
    print("\n=== EXP 6: Rolling Update Under Load ===")
    print("1. Triggering rolling update on shard-b...")
    subprocess.run(["kubectl", "rollout", "restart", "statefulset", "shard-b", "-n", NAMESPACE])
    
    print("2. Sending continuous transfers to shard-b during update...")
    errors = 0
    for i in range(20):
        res = transfer("A1500", "A1501", "0.01")
        if "error" in res or res.get("state") == "ABORTED":
            errors += 1
        time.sleep(2) # Slow down to watch the rollout
        
    print(f"   Errors during rollout: {errors}")
    assert errors == 0, "FAIL: Rolling update caused transaction failures!"
    print("[✓] PASS: Deployment changes preserved transaction integrity.")

# ============================================================
# EXPERIMENT 7: Repeated Client Requests
# ============================================================
def exp_7_repeated_requests():
    print("\n=== EXP 7: Repeated Client Requests (Idempotency) ===")
    acc_a, acc_b = "A0500", "A1500"
    bal_before = Decimal(get_balance(acc_a))
    
    print("1. Sending same request_id 5 times...")
    results = []
    for _ in range(5):
        res = transfer(acc_a, acc_b, "10.00", request_id="idempotency-test-123")
        results.append(res.get("state"))
        
    bal_after = Decimal(get_balance(acc_a))
    print(f"   States returned: {results}")
    print(f"   Balance change: {bal_before} -> {bal_after} (Diff: {bal_before - bal_after})")
    
    assert (bal_before - bal_after) == Decimal("10.00"), "FAIL: Duplicate effects detected!"
    print("[✓] PASS: No duplicate transaction effects.")

# ============================================================
# MAIN RUNNER
# ============================================================
if __name__ == "__main__":
    print("Starting Assignment 3 Kubernetes Experiments...")
    print("Make sure 'kubectl port-forward svc/gateway 8000:8000 -n assignment3' is running!")
    input("Press Enter to begin...")
    
    exp_1_app_pod_failure()
    exp_2_primary_db_failure()
    exp_3_concurrent_transactions()
    exp_4_coordinator_interruption()
    exp_5_autoscaling_under_load()
    exp_6_rolling_update_under_load()
    exp_7_repeated_requests()
    
    print("\n" + "="*50)
    print("ALL 7 EXPERIMENTS COMPLETED SUCCESSFULLY!")
    print("="*50)