import pandas as pd
import pathmng
import matplotlib.pyplot as plt

df = pd.read_csv(pathmng.final_movie_path)
df.info()
df.plot(x='release_year', y='box_office_gross', kind='scatter',
        figsize=(10,6),
        title='House Prices in Melbourne')

plt.show()