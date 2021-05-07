import numpy as np
from sklearn import svm
import pandas
from sklearn.linear_model import LinearRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
import pathmng
import sys

import utils

data = np.load(pathmng.movie_vector_path)

clf = LinearRegression()

X = data[:, :-1]
y = data[:, -1]

# print("Data shape:", data.shape)
print(data)
# breakpoint()

# print(data)

data_train, data_test, labels_train, labels_test = train_test_split(X, y, test_size=0.3)

clf.fit(data_train, labels_train)

test_predicted = clf.predict(data_test)

utils.measure_accuracy(labels_test, test_predicted)