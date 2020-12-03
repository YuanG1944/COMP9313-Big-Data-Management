# version 2
# add groupByKey
# add flag
# by Elijah DengDeng

def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = 0
    numCandidates = 0
    # add groupByKey
    data_hashes = data_hashes.map(lambda x: (tuple(x[1]), x[0])).groupByKey().map(lambda x: (x[0], x[1], False))
    while numCandidates < beta_n: 
        data_hashes = data_hashes.map(lambda x : (x[0], x[1], True if x[2] else isSatisfy( x[0], query_hashes, alpha_m, offset )))
        candidatesRDD = data_hashes.flatMap(lambda x : x[1] if x[2] else [])
        numCandidates = candidatesRDD.count()
        offset += 1
    return candidatesRDD


# check if dataHashCode is qualifited for appending candidates
def isSatisfy(dataHashCode , queryHashCode, alpha_m, offset):
    count = 0
    length = len(dataHashCode)
    for i in range(length):
        if ( abs(dataHashCode[i] - queryHashCode[i]) <= offset):
            count += 1
    return count >= alpha_m