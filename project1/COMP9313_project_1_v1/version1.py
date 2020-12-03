
def isQualified(dataHash, queryHash, alpha_m, offset):
    count = 0
    length = len(dataHash)
    for i in range(length):
        if (abs(dataHash[i] - queryHash[i]) <= offset):
            count += 1
    return count >= alpha_m


def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = 0
    numCandidates = 0
    while numCandidates < beta_n:
        candidateRDD = data_hashes.filter(lambda x : isQualified(x[1], query_hashes, alpha_m, offset)).map(lambda x : x[0])
        numCandidates = candidateRDD.count()
        offset += 1
        print("offset: ", offset,  "numCandidates: ", numCandidates)
    return candidateRDD