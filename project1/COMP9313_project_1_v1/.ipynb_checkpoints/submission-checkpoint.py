## import modules here

########## Question 1 ##########
# do not change the heading of the function
# def diff(data_hash, query_hash):
#         length = len(data_hash)
#         return [abs(data_hash[i] - query_hash[i]) for i in range(length)]

def isQialified(dataHashCode, queryHashCode, alpha_m, offset):
    count = 0
    length = len(dataHashCode)
    for i in range(length):
        if abs(dataHashCode[i] - queryHashCode[i]) <= offset:
           count += 1
        return count >= alpha_m

def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = -1
    numCandidates = -1
    while numCandidates < beta_n:
        offset += 1
        CandidatesRDD = data_hashes.filter(lambda e: isQialified(e[1], query_hashes, alpha_m, offset)).map(lambda e: e[0])
        numCandidates = CandidatesRDD.count()
    
    return CandidatesRDD