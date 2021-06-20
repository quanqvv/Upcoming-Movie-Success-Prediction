import pickle

import numpy as np
from sklearn import svm
from sklearn.model_selection import cross_val_score, train_test_split
import pandas
from sklearn.linear_model import LinearRegression, Lasso, LogisticRegression, Ridge, BayesianRidge
from sklearn.linear_model._sgd_fast import Regression
from sklearn.metrics import accuracy_score, r2_score, mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn.svm import SVC, SVR
import pathmng
import sys
import utils
from evaluate.test_model import test_each_model
from preprocessing import data_model
from preprocessing.data_model import DataModel
import pandas

df = pandas.read_csv(pathmng.final_movie_data_path)
data_model = data_model.get_data_model()

df = df[df.box_office_gross > 0][df.budget > 0]
df["success"] = df["box_office_gross"] > 1.3 * df["budget"]
df = df.sort_values("success", ascending=False)[0:5500]
# df = df.sort_values("release_year", ascending=False)

df_train, df_test = train_test_split(df, test_size=0.3, random_state=False)

del df_test["success"]
df_test["success"] = df["box_office_gross"] > 1.3 * df["budget"]
df_test = df_test.sort_values(by=["release_year", "success"], ascending=False)
df_test.to_csv(pathmng.final_test_movie_data_path, index=False, header=True)

print(df.groupby("success").size())

array_data = []
for index, row in df_train.iterrows():
    array_data.append(data_model.get_full_feature(row))
data_train = np.array(array_data)

array_data = []
for index, row in df_test.iterrows():
    array_data.append(data_model.get_full_feature(row))
data_test = np.array(array_data)

print("Data train shape:", data_train.shape)
print("Data test shape:", data_test.shape)


train_set, test_set, train_label_set, test_label_test = data_train[:, :-1],  data_test[:, :-1], data_train[:, -1], data_test[:, -1]

# train_set, test_set, train_label_set, test_label_test = train_test_split(X, y, test_size=0.3, random_state=False)
# pickle.dump(logreg_model, open(pathmng.logistic_model_path, "wb"))
test_each_model(None, None, train_set, train_label_set, test_set, test_label_test)

breakpoint()

df = pandas.read_csv(pathmng.final_movie_data_path)
data_model = pickle.load(open(pathmng.data_model_path, "rb"))
data_model: DataModel
data_model.show()
import pickle
array_data = []
row_test = None

count = 0
success = 0
real_success = 0
for index, row in df.iterrows():
    if index > 500:
        break
    if index == 30:
        row_test = row
    feature = data_model.get_input_feature(row)
    result = pickle.load(open(pathmng.logistic_model_path, "rb")).predict([feature])
    result = "True" if result[0] == 1 else "False"
    count += 1
    if int(row["box_office_gross"]) > int(row["budget"]):
        real_success += 1
    if result == "True":
        success += 1
    # success_cell.insert(0, result)
print(success/count)
print(real_success/count)