
from random import randint

def randomGenerate(fileName, all_lines, line_length):
    number_of_items_per_line=100000
    f=open(fileName, "w")
    for i in range(all_lines):
        line=""
        for j in range(line_length):
            line =line+" "+ str(randint (1, number_of_items_per_line))
        f.write( line+ "\n")

        if(i % 10000)==0 :
            print (i+0.1)/all_lines
    f.close()

def testRandomGenerate(all=500000, spill_limit=2621, random_range=100000):
    result=[]
    n_current_spill=0
    spill=[]
    for i in range(all):
        spill.append(randint (1, random_range))
        n_current_spill= n_current_spill +1
        if n_current_spill == spill_limit:
            result.append(tuple([len(spill), len(set(spill))]))
            n_current_spill=0
            spill=[]
    result.append(tuple([len(spill), len(set(spill))]))
    return result

def countGroups():
    uset=set()
    total=0
    for i in open("../data/data_10000"):
        s=i.strip().split(" ")
        for item in s:
            item = item.strip()
            item= int(item)
            uset.add(item)
            total= total+1
    print "groups "+ str(len(uset))
    print "total  "+ str(total)


def printGroups(spill):
    total=0
    uset=set()
    combine=0
    for i in open("../data/data_10000"):
        s=i.strip().split(" ")
        for item in s:
            item = item.strip()
            item= int(item)
            uset.add(item)
            total= total+1
            if total==spill :
                print len(uset)
                combine= combine + spill- len(uset)
                total=0
                uset=set()
                

    print "combine "+str(combine)

if __name__ == '__main__':
    result=testRandomGenerate(500000, 2621, 100000)
    for i,j in result:
        print str(j)+",",
    print
    print len(result)
#    countGroups()
#    printGroups(250000)
    #
    s="../data/data_centroids"
    randomGenerate(s, 100, 50)
    print "done"
