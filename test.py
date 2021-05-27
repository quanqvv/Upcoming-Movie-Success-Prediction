import matplotlib.pyplot as plt
# plt.plot([1, 2, 3, 4], [4, 5, 6, 7])
plt.ylim([0, 100])
# plt.plot([7500, 8500, 12000, 14000], [65, 74, 75, 71])
plt.plot([7500, 8500, 12000, 14000], [64, 74, 81, 83])
# plt.plot([7500, 8500, 12000, 14000], [50, 74, 75, 71])
plt.xlabel('Data size')
plt.ylabel("Success rate")

plt.show()