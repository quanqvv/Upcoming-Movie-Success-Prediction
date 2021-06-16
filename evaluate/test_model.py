import pickle

import numpy
import numpy as np
import pickle
from seqeval.metrics import precision_score
from seqeval.metrics.v1 import precision_recall_fscore_support
from sklearn import svm
from sklearn.model_selection import cross_val_score, train_test_split
import pandas
from sklearn.linear_model import LinearRegression, Lasso, LogisticRegression, Ridge, BayesianRidge
from sklearn.linear_model._sgd_fast import Regression
from sklearn.metrics import accuracy_score, r2_score, mean_absolute_error, mean_squared_error, f1_score, recall_score
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn.svm import SVC, SVR
import pickle

import pathmng


def test_each_model(dataX, dataY, trainX, train_labelY, testX, test_labelY):
    max_iter = 10000
    from sklearn.neural_network import MLPClassifier
    svm_model = svm.SVC(kernel='rbf', C=2, gamma=2)
    svm_model2 = svm.SVC(kernel='linear', C=2, gamma=2)
    svm_model3 = svm.SVC(kernel='poly', C=1, gamma=2)

    scores2 = cross_val_score(svm_model, dataX, dataY, cv=5)
    print("SVM_rbf")
    print(scores2)
    # scores3 = cross_val_score(svm_model2, dataX, dataY, cv=5)
    # print("SVM linear")
    # print(scores3)
    scores4 = cross_val_score(svm_model3, dataX, dataY, cv=5)
    print("SVM poly")
    print(scores4)

    from sklearn.linear_model import LogisticRegression
    logreg_model = LogisticRegression(C=1e5, max_iter=1000)
    logreg_model.fit(trainX, train_labelY)
    print("logistic Regression")
    print(logreg_model.score(testX, test_labelY))

    from sklearn.naive_bayes import GaussianNB
    clf = GaussianNB()
    scores5 = cross_val_score(clf, dataX, dataY, cv=5)
    print("NB")
    print(scores5)
    from sklearn.neural_network import MLPClassifier
    mlp_model = MLPClassifier(hidden_layer_sizes=(15,), activation='identity', max_iter=max_iter)
    scores6 = cross_val_score(mlp_model, dataX, dataY, cv=5)
    print("mlp1")
    print(scores6)
    mlp_model2 = MLPClassifier(hidden_layer_sizes=(15,), activation='logistic', max_iter=max_iter)
    scores7 = cross_val_score(mlp_model2, dataX, dataY, cv=5)
    print("mlp2")
    print(scores7)
    mlp_model3 = MLPClassifier(hidden_layer_sizes=(15,), activation='tanh', max_iter=max_iter)
    scores8 = cross_val_score(mlp_model3, dataX, dataY, cv=5)
    print("mlp3")
    print(scores8)
    mlp_model4 = MLPClassifier(hidden_layer_sizes=(15,), activation='relu', max_iter=max_iter)
    scores9 = cross_val_score(mlp_model4, dataX, dataY, cv=5)
    print("mlp4")
    print(scores9)
    mlp_model5 = MLPClassifier(hidden_layer_sizes=(20,), activation='identity', max_iter=max_iter)
    scores10 = cross_val_score(mlp_model5, dataX, dataY, cv=5)
    print("mlp5")
    print(scores10)
    mlp_model6 = MLPClassifier(hidden_layer_sizes=(20,), activation='logistic', max_iter=max_iter)
    scores11 = cross_val_score(mlp_model6, dataX, dataY, cv=5)
    print("mlp6")
    print(scores11)
    mlp_model7 = MLPClassifier(hidden_layer_sizes=(20,), activation='tanh', max_iter=max_iter)
    scores12 = cross_val_score(mlp_model7, dataX, dataY, cv=5)
    print("mlp7")
    print(scores12)
    mlp_model8 = MLPClassifier(hidden_layer_sizes=(20,), activation='relu', max_iter=max_iter)
    scores13 = cross_val_score(mlp_model8, dataX, dataY, cv=5)
    print("mlp8")
    print(scores13)

    # predicted = logreg_model.predict(testX)
    # count = 0
    # for i in range(len(predicted)):
    #     if (predicted[i] - test_labelY[i]) == 0:
    #         count += 1
    # print(float(count) / float(len(predicted)))
    scores = cross_val_score(logreg_model, dataX, dataY, cv=5)
    print(scores)


def test_each_model(dataX, dataY, train_set, train_label_set, test_set, test_label_set):
    max_iter = 10000
    from sklearn.linear_model import LogisticRegression
    from sklearn.neural_network import MLPClassifier
    from sklearn.naive_bayes import GaussianNB
    from sklearn import tree

    # print(numpy.)

    logreg_model = LogisticRegression(C=1e5, max_iter=1000)
    decision_tree = tree.DecisionTreeClassifier(max_depth=7)
    svm_model = svm.SVC(kernel='rbf', C=2, gamma=2)
    svm_model2 = svm.SVC(kernel='linear', C=2, gamma=2)
    svm_model3 = svm.SVC(kernel='poly', C=1, gamma=2)
    clf = GaussianNB()
    # mlp_model = MLPClassifier(hidden_layer_sizes=(15,), activation='identity', max_iter=max_iter)
    # mlp_model2 = MLPClassifier(hidden_layer_sizes=(15,), activation='logistic', max_iter=max_iter)
    # mlp_model3 = MLPClassifier(hidden_layer_sizes=(15,), activation='tanh', max_iter=max_iter)
    # mlp_model4 = MLPClassifier(hidden_layer_sizes=(15,), activation='relu', max_iter=max_iter)
    mlp_model5 = MLPClassifier(hidden_layer_sizes=(20,), activation='identity', max_iter=max_iter)
    mlp_model6 = MLPClassifier(hidden_layer_sizes=(20,), activation='logistic', max_iter=max_iter)
    mlp_model7 = MLPClassifier(hidden_layer_sizes=(20,), activation='tanh', max_iter=max_iter)
    mlp_model8 = MLPClassifier(hidden_layer_sizes=(20,), activation='relu', max_iter=max_iter)

    model_to_name = {
        logreg_model: "Logistic Regression",
        tree.DecisionTreeClassifier(max_depth=8): "Decision Tree (depth = 8)",
        svm_model: "SVM (Kernel = RBF)",
        svm_model2: "SVM (Kernel = Linear)",
        svm_model3: "SVM (Kernel = Poly)",
        clf: "Gaussian Naive Bayes",
        # mlp_model: "MLP identity",
        # mlp_model2: "MLP logistic",
        # mlp_model3: "MLP tanh",
        # mlp_model4: "MLP relu",
        mlp_model5: "MLP (Act = Identity)",
        mlp_model6: "MLP (Act = Logistic)",
        mlp_model7: "MLP (Act = Tanh)",
        mlp_model8: "MLP (Act = Relu)",
    }

    results = []

    for model, name in list(model_to_name.items()):

        model.fit(train_set, train_label_set)
        # print(model.score(test_set, test_label_set))

        y_pred = model.predict(test_set)
        # if model == logreg_model:
        #     df = pandas.DataFrame(data=test_label_set, columns=["y_true"])
        #     df2 = pandas.DataFrame(data=y_pred, columns=["y_pred"])
        #     pandas.concat([df, df2], axis=1).to_csv("result.csv")
        pickle.dump(model_to_name, open(pathmng.model_list_path, "wb"))

        recall = recall_score(y_true=test_label_set, y_pred=y_pred)
        f1 = f1_score(y_true=test_label_set, y_pred=y_pred)
        precision = 1/(2/f1 - 1/recall)
        result = (model.score(test_set, test_label_set), precision, recall, f1)
        result = [round(_, 3) for _ in result]
        result = [name] + result
        results.append(result)
        print(f"Score {name}:",  result)

    print(pandas.DataFrame(results, columns=["Model", "Accuracy", "Precision", "Recall", "F1"]).to_json())



# {"Model":{"0":"Logistic Regression","1":"Decision Tree (max_depth = 8)","2":"SVM (Kernel = RBF)","3":"SVM (Kernel = Linear)","4":"SVM (Kernel = Poly)","5":"Gaussian Naive Bayes","6":"MLP (Act = Identity)","7":"MLP (Act = Logistic)","8":"MLP (Act = Tanh)","9":"MLP (Act = Relu)"},"Accuracy":{"0":0.689,"1":0.63,"2":0.589,"3":0.632,"4":0.577,"5":0.619,"6":0.685,"7":0.671,"8":0.619,"9":0.622},"Precision":{"0":0.712,"1":0.627,"2":0.578,"3":0.742,"4":0.605,"5":0.593,"6":0.717,"7":0.702,"8":0.643,"9":0.65},"Recall":{"0":0.681,"1":0.725,"2":0.796,"3":0.454,"4":0.549,"5":0.75,"6":0.658,"7":0.645,"8":0.611,"9":0.604},"F1":{"0":0.696,"1":0.672,"2":0.67,"3":0.564,"4":0.576,"5":0.662,"6":0.686,"7":0.672,"8":0.626,"9":0.626}}


