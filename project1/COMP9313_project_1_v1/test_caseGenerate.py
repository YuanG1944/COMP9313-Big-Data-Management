import pickle
import random

one_arr = [[1 for _ in range(32)] for _ in range(500000)] #32个0, 5w个
mfive_five = [[random.randint(-4, 6) for _ in range(32)] for _ in range(200000)]
otRand = [[random.randint(20, 40) for _ in range(32)] for _ in range(300000)]
arr = one_arr + mfive_five + otRand
random.shuffle(arr)

with open("testQueryHash100w_32.pkl", 'wb') as f:
    pickle.dump([1 for _ in range(32)], f)
    
with open("testCase100w_32.pkl", 'wb') as f:
    pickle.dump(arr, f)

