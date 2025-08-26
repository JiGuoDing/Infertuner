import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression


# 1️⃣ 生成映射表并保存为文件
def generate_mapping(csv_file="/home/jgd/workplace/Infertuner/data/submit_job_Falcon3-7B-Instruct_1000ms/merged_results.csv",
                     output_file="/home/jgd/workplace/Infertuner/data/submit_job_Falcon3-7B-Instruct_1000ms/parallelism_mapping.csv"):
    df = pd.read_csv(csv_file)
    parallelism_values = sorted(df['parallelism'].unique())
    agg = df.groupby('parallelism')[['throughput_rps', 'avg_latency_ms']].mean().reset_index()

    # 拟合缺失 parallelism
    min_p, max_p = min(parallelism_values), max(parallelism_values)
    full_parallelism = range(min_p, max_p + 1)

    X = np.array(agg['parallelism']).reshape(-1, 1)
    y_throughput = np.array(agg['throughput_rps'])
    y_latency = np.array(agg['avg_latency_ms'])

    throughput_model = LinearRegression().fit(X, y_throughput)
    latency_model = LinearRegression().fit(X, y_latency)

    mapping_list = []
    for p in full_parallelism:
        row = agg[agg['parallelism'] == p]
        if not row.empty:
            throughput = row['throughput_rps'].values[0]
            latency = row['avg_latency_ms'].values[0]
        else:
            throughput = throughput_model.predict([[p]])[0]
            latency = latency_model.predict([[p]])[0]
        mapping_list.append([p, throughput, latency])

    # 保存到 CSV
    mapping_df = pd.DataFrame(mapping_list, columns=['parallelism', 'throughput_rps', 'avg_latency_ms'])
    mapping_df.to_csv(output_file, index=False)
    print(f"Mapping table saved to {output_file}")


if __name__ == "__main__":
    generate_mapping()
