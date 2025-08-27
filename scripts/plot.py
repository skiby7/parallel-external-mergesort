import os
import re
import sys
import numpy as np
import plotly.graph_objects as go
from collections import defaultdict

# Base name (no extension)
base = sys.argv[1]

# Prepare paths
paths = {
    'seq':        base + '.log',
    'mpi_strong': base + '_mpi_strong.log',
    'mpi_weak':   base + '_mpi_weak.log',
}

def read_log(path):
    try:
        return open(path).read()
    except IOError:
        print(f"Warning: could not open {path}")
        return ""

# Load content
logs = {k: read_log(p) for k, p in paths.items()}

# Common regexes
time_pattern   = r"# elapsed time \((.*?)\):\s*([0-9.]+)s"
section_pattern = r"^\((.*?)\)\s*((?:# elapsed time.*\n?)+)"

# --- 1) Sequential + OMP / FastFlow (unchanged) ---
seq_binary_times = []
seq_kway_times   = []
omp_times        = defaultdict(list)
ff_times         = defaultdict(list)
ff_nm_times      = defaultdict(list)



for header, body in re.findall(section_pattern, logs['seq'], re.MULTILINE):
    if 'sequential binary' in header:
        seq_binary_times += [float(t) for _, t in re.findall(time_pattern, body)]
    elif 'sequential kway' in header:
        seq_kway_times  += [float(t) for _, t in re.findall(time_pattern, body)]
    else:
        th = re.search(r"nthreads=(\d+)", header)
        if not th: continue
        nthreads = int(th.group(1))
        for algo, t in re.findall(time_pattern, body):
            if algo == 'mergesort_omp':
                omp_times[nthreads].append(float(t))
            elif algo == 'mergesort_ff':
                ff_times[nthreads].append(float(t))
            elif algo == 'mergesort_ff_no_mapping':
                ff_nm_times[nthreads].append(float(t))

seq_binary_avg = np.mean(seq_binary_times)
seq_kway_avg   = np.mean(seq_kway_times)
best_seq       = min(seq_binary_avg, seq_kway_avg)

omp_avg      = {t: np.mean(v) for t, v in omp_times.items()}
ff_avg       = {t: np.mean(v) for t, v in ff_times.items()}
ff_nm_avg    = {t: np.mean(v) for t, v in ff_nm_times.items()}
omp_speedup  = {t: best_seq/v for t, v in omp_avg.items()}
ff_speedup   = {t: best_seq/v for t, v in ff_avg.items()}
ff_nm_speedup= {t: best_seq/v for t, v in ff_nm_avg.items()}
threads_all  = sorted(set(omp_speedup) | set(ff_speedup) | set(ff_nm_speedup))

# --- 2) MPI-strong scaling – by nnodes ---
mpi_strong = defaultdict(list)
for header, body in re.findall(section_pattern, logs['mpi_strong'], re.MULTILINE):
    m = re.search(r"nnodes=(\d+)", header)
    if not m:
        continue
    nn = int(m.group(1))
    times = [float(t) for algo, t in re.findall(time_pattern, body) if algo == 'mergesort_mpi']
    mpi_strong[nn] += times

mpi_strong_avg     = {n: np.mean(v) for n, v in mpi_strong.items()}
mpi_strong_speedup = {n: best_seq / t for n, t in mpi_strong_avg.items()}
nodes_strong       = sorted(mpi_strong_speedup)

# --- 3) MPI-weak scaling – by nnodes & filesize ---
weak_entries = []
for header, body in re.findall(section_pattern, logs['mpi_weak'], re.MULTILINE):
    m = re.search(r"nnodes=(\d+).*?filesize=(\d+)", header)
    if not m:
        continue
    nn, fs = map(int, m.groups())
    times = [float(t) for algo, t in re.findall(time_pattern, body) if algo == 'mergesort_mpi']
    if times:
        weak_entries.append((nn, fs, np.mean(times)))

# sort and pick first as baseline
weak_entries.sort(key=lambda x: x[0])
nodes_weak = []
mpi_weak_speedup = {}
if len(weak_entries) > 0:
    base_nodes, base_fs, base_time = weak_entries[0]
    print(base_time)
    for nn, fs, t in weak_entries:
        t_ideal = base_time * (fs / base_fs)
        # mpi_weak_speedup[nn] = t_ideal / t
        mpi_weak_speedup[nn] = base_time / t
    print(mpi_weak_speedup)
    nodes_weak = sorted(mpi_weak_speedup)


# === Plot everything together ===
fig = go.Figure()

# OMP / FastFlow
fig.add_trace(go.Scatter(
    x=threads_all,
    y=[omp_speedup[t] for t in threads_all],
    mode='lines+markers',
    name='OMP'
))
fig.add_trace(go.Scatter(
    x=threads_all,
    y=[ff_speedup[t] for t in threads_all],
    mode='lines+markers',
    name='FastFlow'
))

fig.add_trace(go.Scatter(
    x=threads_all,
    y=[ff_nm_speedup[t] for t in threads_all],
    mode='lines+markers',
    name='FastFlow (with no mapping)'
))

# MPI strong
fig.add_trace(go.Scatter(
    x=nodes_strong,
    y=[mpi_strong_speedup[n] for n in nodes_strong],
    mode='lines+markers',
    name='MPI Strong'
))


# Ideal line
max_workers = max(threads_all + nodes_strong + nodes_weak)
fig.add_trace(go.Scatter(
    x=list(range(1, max_workers+1)),
    y=list(range(1, max_workers+1)),
    mode='lines',
    name='Ideal',
    line=dict(dash='dash')
))

fig.update_layout(
    title="Speedup Comparison: OMP, FastFlow, MPI Strong & Weak",
    xaxis_title="Workers (threads or nodes)",
    yaxis_title="Speedup",
    template="plotly_white"
)
if os.getenv("PLOT"):
    fig.show()

# === Sequential binary vs k-way ===
fig2 = go.Figure(data=[
    go.Bar(name='Binary', x=['Binary'], y=[seq_binary_avg]),
    go.Bar(name='K-Way',  x=['K-Way'],  y=[seq_kway_avg])
])
fig2.update_layout(
    title="Sequential Merge: Binary vs K-Way",
    yaxis_title="Time (s)",
    template="plotly_white"
)
if os.getenv("PLOT"):
    fig2.show()

# === Separate plot: MPI-weak actual vs ideal ===
# For weak scaling, ideal speedup grows linearly with the number of nodes
ideal_weak = {n: n for n in nodes_weak}

fig_weak = go.Figure()

# Actual MPI-weak speedup
fig_weak.add_trace(go.Scatter(
    x=nodes_weak,
    y=[mpi_weak_speedup[n] for n in nodes_weak],
    mode='lines+markers',
    name='MPI Weak Actual'
))

# Ideal weak speedup
fig_weak.add_trace(go.Scatter(
    x=nodes_weak,
    y=[ideal_weak[n] for n in nodes_weak],
    mode='lines',
    name='Ideal Weak',
    line=dict(dash='dash')
))

fig_weak.update_layout(
    title="MPI Weak Scaling: Actual vs Ideal",
    xaxis_title="Number of Nodes",
    yaxis_title="Speedup",
    template="plotly_white"
)
if os.getenv("PLOT"):
    fig_weak.show()

print (f"#let {os.path.basename(base)}_speed = (")
print("  threads: ", tuple(threads_all), "\b,")
print("  omp: ", tuple([round(float(i), 2) for i in omp_speedup.values()]), "\b,")
print("  ff: ", tuple([round(float(i), 2) for i in ff_speedup.values()]), "\b,")
print("  ff_nm: ", tuple([round(float(i), 2) for i in ff_nm_speedup.values()]), "\b,")
print("  mpi_strong: ", tuple([round(float(i), 2) for i in mpi_strong_speedup.values()]), "\b,")
print("  mpi_weak: ", tuple([round(float(i), 2) for i in mpi_weak_speedup.values()]), "\b,")
print("  ylim: 10,")
print("  width: 6cm,")
print("  height: 5cm,")
print("  title: [],")
print(")")

omp_eff      = {t: omp_speedup[t]/t for t in omp_speedup}
ff_eff       = {t: ff_speedup[t]/t for t in ff_speedup}
ff_nm_eff    = {t: ff_nm_speedup[t]/t for t in ff_nm_speedup}
mpi_strong_eff = {n: mpi_strong_speedup[n]/n for n in mpi_strong_speedup}
mpi_weak_eff   = {n: mpi_weak_speedup[n]/n for n in mpi_weak_speedup}

print(f"#let {os.path.basename(base)}_eff = (")
print("  threads: ", tuple(threads_all), "\b,")
print("  omp: ", tuple([round(float(i), 2) for i in omp_eff.values()]), "\b,")
print("  ff: ", tuple([round(float(i), 2) for i in ff_eff.values()]), "\b,")
print("  ff_nm: ", tuple([round(float(i), 2) for i in ff_nm_eff.values()]), "\b,")
print("  mpi_strong: ", tuple([round(float(i), 2) for i in mpi_strong_eff.values()]), "\b,")
print("  mpi_weak: ", tuple([round(float(i), 2) for i in mpi_weak_eff.values()]), "\b,")
print("  ylim: 10,")
print("  width: 6cm,")
print("  height: 5cm,")
print("  title: [],")
print(")")
