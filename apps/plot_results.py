import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv('./data/experiments_log.csv')

sns.set_theme(style="whitegrid")
plt.figure(figsize=(10, 6))

ax = sns.barplot(
    data=df, 
    x="dn_count", 
    y="time_sec", 
    hue="optimized",
    palette="viridis"
)

plt.title('Сравнение производительности')
plt.xlabel('Количество DataNodes')
plt.ylabel('Время выполнения (сек)')
plt.legend(title='Spark Optimized')

plt.savefig('../performance_results.png')
plt.show()