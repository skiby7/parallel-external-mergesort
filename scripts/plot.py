import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

def load_and_preprocess_data():
    """Load all CSV files and preprocess into speedup DataFrames"""
    # Load data
    seq_comp = pd.read_csv("sequential_compression.csv")
    seq_decomp = pd.read_csv("sequential_decompression.csv")
    par_comp = pd.read_csv("parallel_compression.csv")
    par_decomp = pd.read_csv("parallel_decompression.csv")

    # Melt parallel data to long format
    def process_parallel(df, operation):
        df = df.melt(id_vars=["Dataset"], var_name="nthreads", value_name="time")
        df["nthreads"] = df["nthreads"].astype(int)
        df["operation"] = operation
        return df

    par_comp_long = process_parallel(par_comp, "compression")
    par_decomp_long = process_parallel(par_decomp, "decompression")

    # Merge with sequential times
    def add_speedup(par_df, seq_df, operation):
        merged = par_df.merge(
            seq_df.rename(columns={"1": "seq_time"}),  # Rename sequential time column
            on="Dataset"
        )
        merged["speedup"] = merged["seq_time"] / merged["time"]
        return merged.drop(columns=["seq_time"])

    speedup_comp = add_speedup(par_comp_long, seq_comp, "compression")
    speedup_decomp = add_speedup(par_decomp_long, seq_decomp, "decompression")

    return pd.concat([speedup_comp, speedup_decomp])

def print_speedup_tables(full_df):
    """Print speedup tables in markdown format"""
    for operation in ["compression", "decompression"]:
        op_df = full_df[full_df["operation"] == operation]
        pivot_df = op_df.pivot_table(
            index="Dataset",
            columns="nthreads",
            values="speedup",
            aggfunc="mean"
        ).round(2)
        transposed = pivot_df.T
        print(f"\n### {operation.capitalize()} Speedup Table\n")
        print(transposed.to_markdown(floatfmt=".2f"))

def plot_speedup_curves(full_df):
    """Create interactive speedup plots with ideal line"""
    fig = px.line(
        full_df,
        x="nthreads",
        y="speedup",
        markers=True,
        color="Dataset",
        facet_col="operation",
        labels={"speedup": "Speedup", "nthreads": "Number of Threads"},
        title="Compression and Decompression Speedup"
    )

    # Add ideal speedup line
    max_threads = full_df["nthreads"].max()
    fig.add_shape(
        type="line",
        x0=1, y0=1,
        x1=max_threads,
        y1=max_threads,
        line=dict(color="black", dash="dot"),
        row="all", col="all"
    )
    line_ticks = list([2**i for i in range(2,7)])
    for i in line_ticks:
        fig.add_vline(x=i, line=dict(color="lightgray", dash="dot"))
    current_ticks = list(set(full_df["nthreads"]))

    combined_ticks = sorted(set((current_ticks) + line_ticks))
    fig.update_layout()
    fig.update_xaxes(tickvals=combined_ticks)
    fig.show()

def main():
    # Load and process data
    full_df = load_and_preprocess_data()

    # Generate outputs
    print_speedup_tables(full_df)
    plot_speedup_curves(full_df)

if __name__ == "__main__":
    main()
