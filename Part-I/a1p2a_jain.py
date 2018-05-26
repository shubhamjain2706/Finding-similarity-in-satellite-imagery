########################################
## Big Data Analytics
## assignment 1 - part II (a), at Stony Brook University
## Fall 2017

## by Shubham Kumar Jain (SBU ID: 111482623)

from pyspark import SparkContext, SparkConf
from random import random



def punctuation(x):
    
    punctuation=(",", "'","\r","\t", "\n", ".", "!", "?", "(", ")", ":", "\"", "/", "\\", "<", ">", "#", "$", "-")
    
    for p in punctuation:
        x = x.replace(p,' ')
    return x


# Function for wordcount
def wordcount(data):
    
    filteredtext=[]
    
    for (k,v) in data:
        filteredtext.append(v)
    
    rdd=sc.parallelize(filteredtext)
   
    rdd=rdd.flatMap(lambda x: [punctuation(x)])
        
    rdd1=rdd.flatMap(lambda line: line.lower().split(" ")).map(lambda word: (word, 1))
    
    
    print("\n\n*****************\n Word Count\n*****************\n")
    print("\nAfter Map Task Completion: ")
    print(rdd1.collect())

    rdd2 = rdd1.reduceByKey(lambda v1,v2:v1 +v2)

    wordcount = rdd2.sortByKey(True)
    
    print("\nAfter Reduce Task Completion: ")
    print(wordcount.collect())
    

    
#Function for set difference    
def setdifference(data):
    
    filteredtext=[]
    
    for (k,v) in data:
        filteredtext.append(v)
    
    rdd3=sc.parallelize(filteredtext)
    
    
    def function(x):
        if(isinstance(x,str)):
            return x.lower()
        else:
            return x
    
    
    rdd4=rdd3.flatMap(lambda x: function(x)).map(lambda word: (word, 1))
    
    
    print("\n\n*****************\n Set Difference\n*****************\n")
    print("\nAfter Map Task Completion: ")
    print(rdd4.collect())
    
    rdd5 = rdd4.reduceByKey(lambda v1,v2: v1+v2)

    setdiff = rdd5.filter(lambda x: x[1]==1 and x[0] in data[0][1]).map(lambda x: x[0])
    
    
    print("\nAfter Reduce Task Completion: ")
    print(setdiff.collect())


if __name__ == "__main__":

    
    conf= SparkConf()
    conf.setAppName("Different Implementations")
    sc = SparkContext(conf=conf)
    
    #WordCount Implementation Below
    
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
			(9, "The car raced past the finish line just in time."),
			(10, "Car engines purred and the tires burned.")]
    
    wordcount(data)
    
    #Set Difference Implementation Below
    
    data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']),
			 ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
	
    data2 = [('R', [x for x in range(50) if random() > 0.5]),
	 		 ('S', [x for x in range(50) if random() > 0.75])]
    
    
    setdifference(data1)
    setdifference(data2)
    
    
    
    
    
