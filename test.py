from sklearn.metrics import mean_squared_error
import numpy as np

# Given values
Y_true = np.array([1, 1, 2, 2, 1]).T # Y_true = Y (original values)

# calculated values
Y_pred = np.array([0.6, 1.29, 1.99, 2.69, 3000.4]).T # Y_pred = Y'

# Calculation of Mean Squared Error (MSE)
print(mean_squared_error(Y_true, Y_pred))