## import modules here
def absDiff(data_hash, query_hash):
    length = len(data_hash)
    res_arr=[]
    for i in range(length):
        res_arr.append(abs(data_hash[i] - query_hash[i]))
    return res_arr

def isSatisfy(difference, alpha_m, offset):
    num = 0
    for value in difference:
        if value <= offset:
            num += 1
        if num >= alpha_m:
            return True
    return False

def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    numCandidates = 0
    data_hashes = data_hashes.map(lambda x : (x[0], absDiff(x[1], query_hashes)))
    data_Maxcode = data_hashes.flatMap(lambda x : [max(x[1])]).max()
    left = 0
    right = data_Maxcode
    while left < right:
        offset = (left + right) // 2
        candidatesRDD = data_hashes.flatMap( lambda x : [x[0]] if isSatisfy(x[1], alpha_m, offset) else [])
        numCandidates = candidatesRDD.count()

        if numCandidates == beta_n:
            print("offset: ", offset,  "numCandidates: ", numCandidates)
            break

        if numCandidates <= beta_n:
            left = offset + 1
            print("offset: ", offset,  "numCandidates: ", numCandidates)
        else:
            right = offset
            print("offset: ", offset,  "numCandidates: ", numCandidates)
    
    if numCandidates < beta_n:
        offset += 1
        candidatesRDD = data_hashes.flatMap( lambda x : [x[0]] if isSatisfy(x[1], alpha_m, offset) else [])
        numCandidates = candidatesRDD.count()

    return candidatesRDD