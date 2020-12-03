R = 20000000
N = 500
K = 200
B = R / N

dp = [['*' for _ in range(K + 1)] for _ in range(6)]

def calculate(dp, index, k):
    if index == 1:
        dp[index][k] = k * B - 2 * dp[2][k] - 3 * dp[3][k] - 4 * dp[4][k] - 5 * dp[5][k]
        return
    dp[index][k] = ((6 - index) * dp[index - 1][k - 1]) / (N - k + 1) \
                 + dp[index][k - 1] \
                 - ((6 - index - 1) * dp[index][k - 1]) / (N - k + 1)

dp[1][1] = B;
for i in range(2, 6):
    dp[i][1] = 0
# for i in dp:
#     print(i)

for i in range(2, K + 1):
    for j in [5, 4, 3, 2, 1]:
        calculate(dp, j, i)

for i in dp:
    print(i)



# R = 20000000
# N = 4000
# K = 3
# B = 750

# dp = [[0 for _ in range(K + 1)] for _ in range(4)]

# def calculate(dp, index, k):
#     if index == 1:
#         dp[index][k] = k * B - 2 * dp[2][k] - 3 * dp[3][k]
#         return
#     dp[index][k] = (4 - index) * dp[index - 1][k - 1] / (N - k + 1) \
#                  + dp[index][k - 1] \
#                  - (3 - index) * dp[index][k - 1] / (N - k - 1)

# dp[1][1] = B;
# for i in range(2, K + 1):
#     for j in [3, 2, 1]:
#         calculate(dp, j, i)

# for i in dp:
#     print(i)