import numpy as np
from sklearn import svm
import pandas
from sklearn.linear_model import LinearRegression, Lasso, LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC

import pathmng
import sys

import utils

data = np.load(pathmng.movie_vector_path, allow_pickle=True)

print("Data shape:", data.shape)

num_label = 4
X = data[:, :-num_label]


for i in range(1, num_label+1)[::-1]:
    y = data[:, -i]
    clf = LinearRegression()
    # clf = SVC()
    # clf = Lasso(alpha=1)
    data_train, data_test, labels_train, labels_test = train_test_split(X, y, test_size=0.2, random_state=False)
    clf.fit(data_train, labels_train)
    # utils.measure_accuracy(clf.predict(data_test), labels_test)
    print(clf.score(data_test, labels_test))