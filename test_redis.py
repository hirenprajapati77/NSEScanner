import time
from scanner import run_scan

print("--- STARTING FIRST SCAN (COLD) ---")
start = time.time()
run_scan()
print(f"First scan took {time.time() - start:.2f} seconds")

print("\n--- STARTING SECOND SCAN (WARM) ---")
start2 = time.time()
run_scan()
print(f"Second scan took {time.time() - start2:.2f} seconds")
