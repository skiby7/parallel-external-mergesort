import os
import sys
from collections import defaultdict

def parse_dataset(path):
    if os.path.basename(path) == "test_files":
        return 'mixed_files'
    parts = path.split('/')
    return parts[-1]

def main(input_filename):
    sequential_compression = defaultdict(list)
    sequential_decompression = defaultdict(list)
    parallel_compression = defaultdict(lambda: defaultdict(list))  # dataset -> {thread: [times]}
    parallel_decompression = defaultdict(lambda: defaultdict(list))

    with open(input_filename, 'r') as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("Running"):
            parts = line.split()
            tool = parts[1]
            dataset_path = parts[3]
            dataset = parse_dataset(dataset_path)

            if tool == 'minizseq':
                nthreads = 1
            else:
                nthreads_part = parts[-1].strip('()')
                nthreads = int(nthreads_part.split('=')[1])

            # Read next two lines for times
            i += 1
            if i >= len(lines):
                break
            time_line1 = lines[i].strip()
            time1 = float(time_line1.split(': ')[1].rstrip('s'))
            i += 1
            if i >= len(lines):
                break
            time_line2 = lines[i].strip()
            time2 = float(time_line2.split(': ')[1].rstrip('s'))

            # Update data structures with collected times
            if tool == 'minizseq':
                sequential_compression[dataset].append(time1)
                sequential_decompression[dataset].append(time2)
            else:
                parallel_compression[dataset][nthreads].append(time1)
                parallel_decompression[dataset][nthreads].append(time2)

            i += 1
        else:
            i += 1

    # Function to write CSV files with average times
    def write_csv(filename, data, threads_list=None):
        datasets = sorted(data.keys())
        if threads_list is None:
            # Handling sequential data (each dataset has a list of times)
            with open(filename, 'w') as f:
                f.write("Dataset,1\n")
                for dataset in datasets:
                    times = data[dataset]
                    avg = sum(times) / len(times) if times else ''
                    f.write(f"{dataset},{avg:.6f}\n" if avg != '' else f"{dataset},\n")
        else:
            # Handling parallel data (each dataset has a dict of thread lists)
            with open(filename, 'w') as f:
                header = "Dataset," + ",".join(map(str, sorted(threads_list))) + "\n"
                f.write(header)
                for dataset in datasets:
                    row = [dataset]
                    thread_data = data.get(dataset, {})
                    for th in sorted(threads_list):
                        times = thread_data.get(th, [])
                        avg = sum(times) / len(times) if times else ''
                        row.append(f"{avg:.6f}" if avg != '' else '')
                    f.write(",".join(row) + "\n")

    # Write sequential files
    write_csv("sequential_compression.csv", sequential_compression)
    write_csv("sequential_decompression.csv", sequential_decompression)

    # Collect all unique threads for parallel files
    pc_threads = set()
    for ds in parallel_compression:
        pc_threads.update(parallel_compression[ds].keys())
    pd_threads = set()
    for ds in parallel_decompression:
        pd_threads.update(parallel_decompression[ds].keys())

    # Write parallel compression and decompression files
    write_csv("parallel_compression.csv", parallel_compression, pc_threads)
    write_csv("parallel_decompression.csv", parallel_decompression, pd_threads)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py input_file")
        sys.exit(1)
    main(sys.argv[1])
