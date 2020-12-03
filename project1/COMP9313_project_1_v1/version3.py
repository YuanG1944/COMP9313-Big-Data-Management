 
## import modules here

# alpha_m  = 10
# beta_n = 10000

def isSatisfy(dataHash, queryHash, alpha_m, offset):
    num = 0
    length = len(dataHash)
    for i in range(length):
        if (abs(dataHash[i] - queryHash[i]) <= offset):
            num += 1
    return num >= alpha_m

def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = 0
    numCandidates = 0
    while numCandidates < beta_n:
        candidatesRDD = data_hashes.flatMap( lambda x : [x[0]] if isSatisfy(x[1], query_hashes, alpha_m, offset) else [])
        numCandidates = candidatesRDD.count()
        print("offset: ", offset,  "numCandidates: ", numCandidates)
        offset += 1 
    return candidatesRDD
