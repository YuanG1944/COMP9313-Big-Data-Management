def Comptent(difference, alpha_m, offset):
    i = 1
    for val in range( len(difference) ):
        if difference[val] <= offset:
            i += 1
        if i > alpha_m:
            return True
    return False


def sub_hash_query(data_hash, query_hash):
    length = len(data_hash)
    list_sub=[]
    for i in range(length):
        list_sub.append(abs(data_hash[i] - query_hash[i]))
    return list_sub


def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
 
    data_hashes = data_hashes.map(lambda s : (s[0], sub_hash_query(s[1], query_hashes)))
    offset = -2
    num_Candidates = 0
    while num_Candidates < beta_n:
        offset += 2  
        RDD_Candidates = data_hashes.flatMap( lambda s : [s[0]] if Comptent(s[1], alpha_m, offset) else [])
        num_Candidates = RDD_Candidates.count()
    offset -= 1
    RDD_Candidates_tmp = data_hashes.flatMap( lambda s : [s[0]] if Comptent(s[1], alpha_m, offset) else [])
    num_Candidates_tmp = RDD_Candidates_tmp.count()  
    
    if num_Candidates_tmp >= beta_n:
        return RDD_Candidates_tmp     
    else:
        offset += 1
        return RDD_Candidates 