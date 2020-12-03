# Binary
# import modules here
######### Question 1 ##########
# do not change the heading of the function
def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    # **replace** this line with your code
    # ### default correct_offset=0

    offset = 0
    numCandidates = 0
    while offset < 3: # 0 1 2
        candidatesRDD = getCandRDD(data_hashes, query_hashes, alpha_m, offset)
        numCandidates = candidatesRDD.count()
        print(offset, numCandidates)
        if numCandidates >= beta_n:
            return candidatesRDD
        offset += 1
    # print("111111")
    # print(offset, numCandidates)


    # ########### largest_offset = 10000 < 2^14
    # offset = 0 #numC(offset=2)
    ceil = 15
    floor = 0
    #### xiugai #####
    candidatesRDD = getCandRDD(data_hashes, query_hashes, alpha_m, offset=2**14)
    numCandidates = candidatesRDD.count()
    # def binarySearch(numCandidates, beta_n, ceil, floor):
    mid = 0
    # print("numCandidates: ", numCandidates)
    while True:
    # while numCandidates >= beta_n:
    #     print("@@@@@@@@@@@")
        mid = half(ceil, floor)
        # print("mid: ", mid)
        # floor = mid
        candidatesRDD = getCandRDD(data_hashes, query_hashes, alpha_m, offset=(2**mid))
        numCandidates = candidatesRDD.count()
        preCand = getCandRDD(data_hashes, query_hashes, alpha_m, offset=(2**(mid-1)))
        pre_num = preCand.count()
        print(2**(mid-1), ": ", pre_num, 2**(mid), ": ", numCandidates)
        # print("#########")
        if pre_num <= beta_n and numCandidates >= beta_n:
            # print("stop")
            break
        if pre_num > beta_n and numCandidates > beta_n:
            ceil = mid
        if pre_num < beta_n and numCandidates < beta_n:
            floor = mid
        # return mid
    if numCandidates == beta_n:
        return candidatesRDD
    if pre_num == beta_n:
        return preCand

    #######################
    ceil = 2 ** mid
    print(mid)
    # print(ceil)
    offset = ceil
    # print(offset)
    floor = ceil // 2
    # print(ceil, floor)
    # print(numCandidates, beta_n)
    alarm = []
    # alarm = 0
    while numCandidates != beta_n:
        if numCandidates > beta_n:
            # print("G1: ", "ceil: ", ceil, "floor: ", floor)
            print("offset: ", offset)
            pre_offset = offset
            offset = half(ceil, floor)
            alarm.append(offset)
            # alarm = numCandidates
            numCandidates = getCandRDD(data_hashes, query_hashes, alpha_m, offset).count()
            print(offset, numCandidates)
            ceil = offset
            # print("G2: ", "ceil: ", ceil, "floor: ", floor)
            # print("$$$$$$$$$$$$$$$$$")
        elif numCandidates < beta_n:
            # print("S1: ", "ceil: ", ceil, "floor: ", floor)
            ceil = pre_offset
            floor = offset
            offset = half(ceil, floor)
            alarm.append(offset)
            # alarm = numCandidates
            numCandidates = getCandRDD(data_hashes, query_hashes, alpha_m, offset).count()
            print(offset, numCandidates)
            # print("S2: ", "ceil: ", ceil, "floor: ", floor)
            # print("*******************")
        else:
            break

        # if alarm >=beta_n and numCandidates < beta_n:
        #     offset += 1
        #     break
        len_alarm = len(alarm)
        if len_alarm > 3:
            if alarm[len_alarm-1] == alarm[len_alarm-2] == alarm[len_alarm-3]:
                    offset += 1
                    break

    # while numCandidates == getCandRDD(data_hashes, query_hashes, alpha_m, offset-1).count():
    #     offset -= 1
    #     numCandidates = getCandRDD(data_hashes, query_hashes, alpha_m, offset).count()
    #     print(offset, numCandidates)
    # print("offset: ", offset)
    # print("Result: ", "offset:", offset, "numCand:", numCandidates)
    return getCandRDD(data_hashes, query_hashes, alpha_m, offset)



def half(ceil, floor):
    offset = (ceil + floor) // 2
    return offset

def getCandRDD(dataHashCode, query_hashes, alpha_m, offset):
    candidatesRDD = dataHashCode \
        .flatMap(lambda e: [e[0]] \
        if ifMatch(e[1], query_hashes, alpha_m, offset) else [])
    return candidatesRDD

def ifMatch(dataHashCode, queryHashCode, alpha_m, offset):
    count = 0
    length = len(dataHashCode)
    for i in range(length):
        if abs(dataHashCode[i] - queryHashCode[i]) <= offset:
            count += 1
    # return bool value
    return count >= alpha_m
#########################################
