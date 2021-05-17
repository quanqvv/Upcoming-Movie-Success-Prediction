import numpy as np
from pyspark.ml.regression import LinearRegression
from sklearn import svm
import pandas
from sklearn.model_selection import train_test_split
import pathmng
import sys

import utils

data = np.load(pathmng.movie_vector_path)

clf = svm.SVC()

X = data[:, :-1]
y = data[:, -2]

print("Data shape:", data.shape)
np.set_printoptions(threshold=sys.maxsize)

print(data)
# breakpoint()

# print(data)

data_train, data_test, labels_train, labels_test = train_test_split(X, y, test_size=0.2)

clf.fit(data_train, labels_train)

# utils.measure_accuracy(clf.predict(data_test), labels_test)

print(clf.predict(data_test))
print(labels_test)

print(clf.score(data_test, labels_test))