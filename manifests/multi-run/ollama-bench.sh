#!/usr/bin/env bash

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────
OLLAMA_HOST="${OLLAMA_HOST:-http://localhost:11434}"
CONCURRENCY="${CONCURRENCY:-4}"
TOTAL_QUERIES="${TOTAL_QUERIES:-20}"
MODEL="${MODEL:-llama3.1:8b}"
RESULTS_DIR="$(mktemp -d)"

# ── Prompt pool — varied lengths and topics ──────────────────────────
PROMPTS=(
  "Explain the concept of gravitational waves in simple terms."
  "Write a Python function that implements binary search on a sorted list. Include docstring and type hints."
  "What are the key differences between TCP and UDP? Give examples of when you would use each."
  "Summarize the plot of Hamlet in exactly three sentences."
  "Describe how a modern CPU pipeline works, including hazard detection and branch prediction."
  "Write a short poem about a robot learning to paint."
  "Explain the CAP theorem in distributed systems. Give a real-world example for each trade-off."
  "What causes the aurora borealis? Explain the physics involved."
  "Compare and contrast REST and GraphQL APIs. When would you choose one over the other?"
  "Explain how transformers work in machine learning, focusing on the attention mechanism."
  "Write a bash one-liner that finds all files larger than 100MB in the current directory tree."
  "What is the significance of the Turing test? Discuss its limitations."
  "Describe the water cycle and explain how climate change is affecting it."
  "Explain the difference between symmetric and asymmetric encryption with examples."
  "Write a SQL query that finds the top 5 customers by total order value, including ties."
  "What are the main challenges in quantum computing today?"
  "Explain containerization versus virtualization. Why did Docker become so popular?"
  "Describe the process of protein folding and why it matters for drug discovery."
  "What is the P vs NP problem and why does it matter?"
  "Explain how garbage collection works in the JVM, covering the major algorithms."
)

# ── Preflight check ──────────────────────────────────────────────────
echo "============================================================"
echo "  Ollama Multi-Client Benchmark"
echo "============================================================"
echo "  Endpoint:      ${OLLAMA_HOST}"
echo "  Model:         ${MODEL}"
echo "  Concurrency:   ${CONCURRENCY} simultaneous clients"
echo "  Total queries: ${TOTAL_QUERIES}"
echo "  Results dir:   ${RESULTS_DIR}"
echo "============================================================"
echo ""

echo -n "Checking Ollama endpoint... "
if ! curl -sf "${OLLAMA_HOST}/api/tags" > /dev/null 2>&1; then
  echo "FAILED"
  echo ""
  echo "ERROR: Cannot reach ${OLLAMA_HOST}/api/tags"
  echo "Make sure port-forward is running:"
  echo "  kubectl -n test port-forward svc/ollama 11434:11434"
  exit 1
fi
echo "OK"
echo ""

send_query() {
  local query_id="$1"
  local prompt_file="$2"
  local outfile="${RESULTS_DIR}/result_$(printf '%03d' "${query_id}").json"
  local prompt
  prompt=$(<"${prompt_file}")

  # Build JSON payload safely via python
  local payload
  payload=$(python3 -c "
import json, sys
print(json.dumps({
    'model': sys.argv[1],
    'prompt': sys.argv[2],
    'stream': False,
    'options': {'num_predict': 256}
}))
" "${MODEL}" "${prompt}")

  local start_s
  start_s=$(python3 -c "import time; print(time.monotonic())")

  local http_code
  http_code=$(curl -s -o "${outfile}" -w "%{http_code}" \
    --max-time 300 \
    "${OLLAMA_HOST}/api/generate" \
    -H "Content-Type: application/json" \
    -d "${payload}" 2>/dev/null) || http_code="000"

  local end_s
  end_s=$(python3 -c "import time; print(time.monotonic())")

  # Parse results
  python3 -c "
import json, sys, os

query_id = int(sys.argv[1])
http_code = sys.argv[2]
start_s = float(sys.argv[3])
end_s = float(sys.argv[4])
outfile = sys.argv[5]
wall_s = end_s - start_s

if http_code == '200' and os.path.exists(outfile):
    try:
        with open(outfile) as f:
            r = json.load(f)
        prompt_tok = r.get('prompt_eval_count', 0)
        gen_tok    = r.get('eval_count', 0)
        prompt_ns  = r.get('prompt_eval_duration', 0)
        gen_ns     = r.get('eval_duration', 0)
        prompt_tps = prompt_tok / (prompt_ns / 1e9) if prompt_ns > 0 else 0
        gen_tps    = gen_tok / (gen_ns / 1e9) if gen_ns > 0 else 0
        print(f'  [query {query_id:3d}]  HTTP {http_code}  wall {wall_s:6.1f}s  '
              f'prompt {prompt_tok:3d} tok ({prompt_tps:6.1f} t/s)  '
              f'gen {gen_tok:3d} tok ({gen_tps:6.1f} t/s)')
    except Exception as e:
        print(f'  [query {query_id:3d}]  HTTP {http_code}  wall {wall_s:6.1f}s  parse error: {e}')
else:
    print(f'  [query {query_id:3d}]  HTTP {http_code}  wall {wall_s:6.1f}s  FAILED')
" "${query_id}" "${http_code}" "${start_s}" "${end_s}" "${outfile}"
}

PROMPT_DIR="${RESULTS_DIR}/prompts"
mkdir -p "${PROMPT_DIR}"
for i in "${!PROMPTS[@]}"; do
  echo "${PROMPTS[$i]}" > "${PROMPT_DIR}/${i}.txt"
done

# run queries
BENCH_START=$(python3 -c "import time; print(time.monotonic())")

echo "Sending ${TOTAL_QUERIES} queries (${CONCURRENCY} concurrent)..."
echo ""

PIDS=()
RUNNING=0

for i in $(seq 1 "${TOTAL_QUERIES}"); do
  prompt_idx=$(( (i - 1) % ${#PROMPTS[@]} ))

  # Wait if we've hit the concurrency limit
  while [ "$(jobs -rp | wc -l | tr -d ' ')" -ge "${CONCURRENCY}" ]; do
    sleep 0.1
  done

  send_query "${i}" "${PROMPT_DIR}/${prompt_idx}.txt" &
  PIDS+=($!)
done

for pid in "${PIDS[@]}"; do
  wait "${pid}" 2>/dev/null || true
done

BENCH_END=$(python3 -c "import time; print(time.monotonic())")

echo ""

python3 -c "
import json, glob, os, sys

results_dir = sys.argv[1]
concurrency = int(sys.argv[2])
total_queries = int(sys.argv[3])
bench_start = float(sys.argv[4])
bench_end = float(sys.argv[5])
wall_clock = bench_end - bench_start

files = sorted(glob.glob(os.path.join(results_dir, 'result_*.json')))

if not files:
    print('No results collected.')
    sys.exit(0)

total_prompt_tok = 0
total_gen_tok = 0
total_prompt_ns = 0
total_gen_ns = 0
total_duration_ns = 0
gen_tps_values = []
successful = 0
failed = 0

for f in files:
    try:
        with open(f) as fh:
            r = json.load(fh)
        ptok = r.get('prompt_eval_count', 0)
        gtok = r.get('eval_count', 0)
        pns  = r.get('prompt_eval_duration', 0)
        gns  = r.get('eval_duration', 0)
        total_prompt_tok += ptok
        total_gen_tok    += gtok
        total_prompt_ns  += pns
        total_gen_ns     += gns
        total_duration_ns += r.get('total_duration', 0)
        if gns > 0:
            gen_tps_values.append(gtok / (gns / 1e9))
        successful += 1
    except Exception:
        failed += 1

print('============================================================')
print('  Aggregate Results')
print('============================================================')
print(f'  Successful queries:    {successful}/{total_queries}')
if failed:
    print(f'  Failed queries:        {failed}')
print(f'  Total prompt tokens:   {total_prompt_tok}')
print(f'  Total generated tokens:{total_gen_tok}')
print()

if total_prompt_ns > 0:
    avg_prompt_tps = total_prompt_tok / (total_prompt_ns / 1e9)
    print(f'  Avg prompt throughput: {avg_prompt_tps:.1f} tokens/sec')
if gen_tps_values:
    avg_gen_tps = sum(gen_tps_values) / len(gen_tps_values)
    min_gen_tps = min(gen_tps_values)
    max_gen_tps = max(gen_tps_values)
    print(f'  Avg gen throughput:    {avg_gen_tps:.1f} tokens/sec  (per-request, no contention)')
    print(f'  Min gen throughput:    {min_gen_tps:.1f} tokens/sec')
    print(f'  Max gen throughput:    {max_gen_tps:.1f} tokens/sec')

print()
print(f'  Concurrency:           {concurrency}')
print(f'  Total wall-clock time: {wall_clock:.1f} s')
if wall_clock > 0 and total_gen_tok > 0:
    effective_tps = total_gen_tok / wall_clock
    print(f'  Effective throughput:  {effective_tps:.1f} tokens/sec  (total gen tokens / wall time)')
    queries_per_min = (successful / wall_clock) * 60
    print(f'  Queries per minute:    {queries_per_min:.1f}')
print()
print('============================================================')
" "${RESULTS_DIR}" "${CONCURRENCY}" "${TOTAL_QUERIES}" "${BENCH_START}" "${BENCH_END}"

echo ""
echo "Raw JSON responses saved in: ${RESULTS_DIR}"
echo "Done."
