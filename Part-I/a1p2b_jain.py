########################################
## Big Data Analytics
## assignment 1 - part II (b), at Stony Brook University
## Fall 2017

## by Shubham Kumar Jain (SBU ID: 111482623)

import sys

from pyspark import SparkContext, SparkConf


def punctuation(x):
    
    punctuation=(",", "'","\r","\t", "\n", ".", "!", "?", "(", ")", ":", "\"", "/", "\\", "<", ">", "#", "$", "-")
    
    for p in punctuation:
        x = x.replace(p,' ')
    return x



if __name__ == "__main__":
    conf=SparkConf()
    conf.setAppName("Blog Corpus")
    sc=SparkContext(conf=conf)
    Files=sc.wholeTextFiles("file:///Users/Shubham/Desktop/SBU/project/blogs/*")
    rdd = Files.map(lambda x: x[0]).map(lambda name: name.lower().split('.')).map(lambda x: x[3]).distinct().map(lambda x: (x,1))
    
    
    #All indusries broadcasted
    s=sc.broadcast(rdd.collectAsMap())
    
    industries=[]
    
    for k in s.value:
        industries.append(k)
        
    print("\n\n*****************\n Broadcasted Industries\n*****************\n")
    print(industries)
    
    Content=Files.map(lambda x: x[1])
    
    rdd=Content.flatMap(lambda x: x.lower().split('<date>')).map(lambda x: x.split('</date>')).filter(lambda x: "blog" not in x[0]).map(lambda x: [x[0].replace(","," "),x[1]])
    
    rdd=rdd.map(lambda x: [x[0],punctuation(x[1])])

    
    rdd=rdd.flatMap(lambda x: [[x[0].split(" "),x[1].split(" ")]])
    rdd1=rdd.map(lambda x: [x[0][1:],[w for w in x[1] if w and w in industries]]).filter(lambda x:x[1]!=[])

    rdd2=rdd1.map(lambda x: [[w,x[0]] for w in x[1]])
    rdd3=rdd2.map(lambda x: [x[0][0],[x[0][1][1],x[0][1][0]]]).map(lambda x: [x[0],'-'.join(x[1])])
    rdd4=rdd3.map(lambda x: ('.'.join(x),1))
    
    
    rdd5 = rdd4.reduceByKey(lambda a,b: a+b)
    rdd6=rdd5.map(lambda x: [x[0].split('.'),x[1]]).map(lambda x: (x[0][0],(x[0][1],x[1]))).sortBy(lambda x: x[1][0])  
    
    rdd7=rdd6.map(lambda x: (x[0], (x[1][0], x[1][1]))).groupByKey()
    
    
    print("\n\n*****************\n Industries with their appearance in the posts according to the year-month\n*****************\n")
    print(rdd7.mapValues(list).collect())
    