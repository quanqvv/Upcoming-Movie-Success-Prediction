import numpy as np
from sklearn import svm
import pandas
from sklearn.linear_model import LinearRegression, Lasso, LogisticRegression, Ridge, BayesianRidge
from sklearn.linear_model._sgd_fast import Regression
from sklearn.metrics import accuracy_score, r2_score
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC, SVR

import pathmng
import sys

import utils

data = np.load(pathmng.movie_full_feature_vector_path, allow_pickle=True)

# data[:, [-3, -2, -1]] = data[:, [-3, -1, -2]]

print("Data shape:", data.shape)
print(data)

num_label = 1
X = data[:, :-num_label]

for i in range(1, num_label+1)[::-1]:
    y = data[:, -i]
    # clf = LinearRegression()
    # clf = Ridge(tol=10, normalize=True)
    # clf = SVR(kernel="precomputed")
    clf = Lasso(alpha=1, tol=1)
    data_train, data_test, labels_train, labels_test = train_test_split(X, y, test_size=0.25, random_state=True)
    clf.fit(data_train, labels_train)
    # utils.measure_accuracy(clf.predict(data_test), labels_test)
    print(clf.score(data_test, labels_test))

    import cpi
