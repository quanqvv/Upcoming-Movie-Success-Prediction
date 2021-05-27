import numpy as np
from sklearn import svm
import pandas
from sklearn.linear_model import LinearRegression, Lasso, LogisticRegression, Ridge, BayesianRidge
from sklearn.linear_model._sgd_fast import Regression
from sklearn.metrics import accuracy_score, r2_score, mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC, SVR

import pathmng
import sys

import utils

data = np.load(pathmng.movie_audience_score_vector_path, allow_pickle=True)[:9000, :]

# data[:, [-3, -2, -1]] = data[:, [-3, -1, -2]]

print("Data shape:", data.shape)

num_label = 1
X = data[:, :-num_label]

# data_model =

for i in range(1, num_label+1)[::-1]:
    y = data[:, -i]
    clf = Lasso(alpha=1, tol=0.1)
    data_train, data_test, labels_train, labels_test = train_test_split(X, y, test_size=0.3, random_state=True)
    clf.fit(data_train, labels_train)
    print(r2_score(labels_test, clf.predict(data_test)))
    utils.measure_accuracy(labels_test, clf.predict(data_test))

