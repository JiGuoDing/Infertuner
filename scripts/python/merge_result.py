import os
import glob
import pandas as pd

# 设置你的工作目录（可选）
os.chdir('/home/jgd/workplace/Infertuner/data/submit_job_Falcon3-7B-Instruct_1000ms/')

# 获取所有匹配的 CSV 文件
csv_files = glob.glob('p*.csv')

# 读取并合并所有文件
df_list = [pd.read_csv(f) for f in sorted(csv_files)]
merged_df = pd.concat(df_list, ignore_index=True)

# 保存合并后的结果
merged_df.to_csv('merged_results.csv', index=False)

print(f"合并完成，共合并了 {len(csv_files)} 个文件，结果保存在 merged_results.csv")