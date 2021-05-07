from collections import Counter
# Given list
listA = [45, 20, 11, 50, 17, 45, 50,13, 45]
print("Given List:\n",listA)
occurence_count = Counter(listA)
res=occurence_count.most_common(2)
print("Element with highest frequency:\n",res)