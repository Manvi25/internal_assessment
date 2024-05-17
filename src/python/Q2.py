list = [3, 0, 1, 4, 6, 2, 7, 10]
n = 10
result = []

for i in range(n + 1):
    if i not in list:
        result.append(i)

print(result)