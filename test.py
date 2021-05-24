import cpi
import pathmng

import pandas

cpi_list = []
for i in range(1970, 2021):
    cpi_list.append((i, cpi.get(i)))

df = pandas.DataFrame(data=cpi_list, columns=["year", "cpi"])
df.to_csv(pathmng.consumer_price_index_path, header=True, index=False)