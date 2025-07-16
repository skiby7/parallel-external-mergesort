import re
import sys
import numpy as np
import plotly.graph_objects as go
from collections import defaultdict

log_path = sys.argv[1]  # Replace with your actual path

# Load log content
with open(log_path, "r") as f:
    log = f.read()

# Store times
seq_binary_times = []
seq_kway_times = []
omp_times = defaultdict(list)
ff_times = defaultdict(list)

# Patterns
time_pattern = r"# elapsed time \((.*?)\): ([0-9.]+)s"
section_pattern = r"^#{33,}\n\((.*?)\)\n((?:# elapsed time.*\n?)+)"

# Parse log sections
for match in re.finditer(section_pattern, log, re.MULTILINE):
    header = match.group(1)
    body = match.group(2)

    if "sequential binary" in header:
        seq_binary_times.extend([float(t) for _, t in re.findall(time_pattern, body)])
    elif "sequential kway" in header:
        seq_kway_times.extend([float(t) for _, t in re.findall(time_pattern, body)])
    else:
        threads_match = re.search(r"nthreads=(\d+)", header)
        if not threads_match:
            continue
        threads = int(threads_match.group(1))
        for algo, t in re.findall(time_pattern, body):
            if algo == "mergesort_omp":
                omp_times[threads].append(float(t))
            elif algo == "mergesort_ff":
                ff_times[threads].append(float(t))

# Compute averages
seq_binary_avg = np.mean(seq_binary_times)
seq_kway_avg = np.mean(seq_kway_times)
best_seq = min(seq_binary_avg, seq_kway_avg)

omp_avg = {k: np.mean(v) for k, v in omp_times.items()}
ff_avg = {k: np.mean(v) for k, v in ff_times.items()}

omp_speedup = {k: best_seq / v for k, v in omp_avg.items()}
ff_speedup = {k: best_seq / v for k, v in ff_avg.items()}

all_threads = sorted(set(omp_speedup.keys()) | set(ff_speedup.keys()))

# === Plot 1: Combined Speedup Plot ===
fig_speedup = go.Figure()

fig_speedup.add_trace(go.Scatter(
    x=all_threads,
    y=[omp_speedup.get(k, None) for k in all_threads],
    mode='lines+markers',
    name='OMP',
    line=dict(color='blue')
))

fig_speedup.add_trace(go.Scatter(
    x=all_threads,
    y=[ff_speedup.get(k, None) for k in all_threads],
    mode='lines+markers',
    name='FastFlow',
    line=dict(color='orange')
))

fig_speedup.add_trace(go.Scatter(
    x=all_threads,
    y=all_threads,
    mode='lines',
    name='Ideal Speedup',
    line=dict(color='gray', dash='dash')
))

fig_speedup.update_layout(
    title="Speedup vs Best Sequential (OMP vs FastFlow)",
    xaxis_title="Number of Threads",
    yaxis_title="Speedup",
    template="plotly_white"
)

fig_speedup.show()

# === Plot 2: Binary vs K-Way Merge Raw Times ===
fig_seq = go.Figure(data=[
    go.Bar(name='Binary Merge', x=['Binary'], y=[seq_binary_avg], marker_color='blue'),
    go.Bar(name='K-Way Merge', x=['K-Way'], y=[seq_kway_avg], marker_color='green')
])

fig_seq.update_layout(
    title="Sequential Merge Times: Binary vs K-Way",
    yaxis_title="Time (s)",
    template="plotly_white"
)

fig_seq.show()
