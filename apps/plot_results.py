import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv('./data/experiments_log.csv')

df['dataset_scale'] = df.groupby('experiment').cumcount() + 1

metrics = ['total_time_sec', 'calc_time_sec', 'total_exec_ram_mb', 'opt_time_sec', 'load_time_sec']
titles = ['Общее время выполнения', 'Время вычислений', 'Потребление RAM (MB)', 'Время на оптимизацию', 'Время на загрузку данных из HDFS']

sns.set_theme(style="whitegrid")

fig, axes = plt.subplots(len(metrics), 1, figsize=(12, 6 * len(metrics)))

for i, metric in enumerate(metrics):
    ax = axes[i] if len(metrics) > 1 else axes
    
    sns.lineplot(
        data=df,
        x="dataset_scale", 
        y=metric,
        hue="experiment",
        marker="o",
        ax=ax
    )
    
    ax.set_title(titles[i], fontsize=14)
    ax.set_xlabel('Размер датасета', fontsize=12)
    ax.set_ylabel('Значение', fontsize=12)
    ax.set_xticks([1, 2, 3])
    ax.legend(title='Конфигурация', bbox_to_anchor=(1.05, 1), loc='upper left')

plt.tight_layout()
plt.savefig('performance_comparison.png', bbox_inches='tight')