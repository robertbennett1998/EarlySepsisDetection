import pandas as pd
import os
import data_extraction as de
from tqdm import tqdm
import time

df_a = pd.DataFrame({"Subject Id" : [1, 1, 1, 1, 1, 2, 2, 2, 2, 2], "id" : [1, 1, 2, 2, 2, 3, 3, 3, 4, 4], "Time" : [1.0, 1.1, 2.0, 3.0, 3.3, 1.0, 2.0, 2.3, 2.5, 3.3], "item" : [1, 1, 2, 1, 1, 1, 2, 1, 1, 1], "value" : [1, 1, 2, 1, 1, 1, 2, 1, 1, 1]})
for df in df_a.groupby(["Subject Id", "id"]):
    print(df, "\n----------------")

print(df_a.join(pd.pivot(df_a, values="value", columns="item")))