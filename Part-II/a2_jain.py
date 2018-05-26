########################################
## Big Data Analytics
## assignment 2 at Stony Brook University
## Fall 2017

## by Shubham Kumar Jain (SBU ID: 111482623)

import io
import numpy as np
import zipfile
import hashlib
from pyspark import SparkContext, SparkConf
from PIL import Image
from scipy import linalg
from tifffile import TiffFile


def getOrthoTif(zfBytes):
    #given a zipfile as bytes (i.e. from reading from a binary file),
    # return a np array of rgbx values for each pixel
    bytesio = io.BytesIO(zfBytes)
    zfiles = zipfile.ZipFile(bytesio, "r")
    #find tif:
    for fn in zfiles.namelist():
        if fn[-4:] == '.tif':#found it, turn into array:
            tif = TiffFile(io.BytesIO(zfiles.open(fn).read()))
    return tif.asarray()


def breakimage(imgarray, reducebyfactor, takemean=0):
    
    imgarray=np.array(imgarray)
    
    img_sets=[]
    
    for x in range(0,imgarray.shape[0],reducebyfactor):
        temp=[]
        for y in range(0,imgarray.shape[1],reducebyfactor):
            subimg=imgarray[x:x+reducebyfactor,y:y+reducebyfactor]
            #takemean=0 for breaking the image in 1(c)
            if takemean==0:
                img_sets.append(subimg)
            #takemean=1 for calculating the mean intensity for the 2(b)
            if takemean==1:
                total=np.sum(subimg)
                mean=total/(reducebyfactor*reducebyfactor)
                temp.append(mean)
        #takemean=1 for calculating the mean intensity for the 2(b)        
        if takemean==1:
            img_sets.append(temp)
    #if takemean=0, return imgsets
    if takemean==0:
        return img_sets
    #if takemean=1, return newly formed array
    if takemean==1:
        return np.array(img_sets)
    
    
def intensity(array):

    intensities=[]
    
    for i in range(array.shape[0]):
        temp=[]
        for j in range(array.shape[1]):
            mean=int((int(array[i][j][0])+int(array[i][j][1])+int(array[i][j][2]))/3)
            intensity=int(mean*int(array[i][j][3])/100)
            temp.append(intensity)
        intensities.append(temp)
            
    return np.array(intensities)  


def diff(array,ax=-1):
    
    reduced=[]
    
    for i in range(array.shape[0]):
        temp=[]
        x=np.diff(array[i],axis=ax)
        temp.append(x)
        for j in range(len(temp)):
            for k in range(len(temp[0])):
                if temp[j][k] < -1:
                    temp[j][k] = -1
                elif temp[j][k] > 1:
                    temp[j][k]=1
                else:
                    temp[j][k]=0
        reduced.append(temp)
    reduced=np.array(reduced)

    return np.array(reduced) 
                    

def hashit(array):
    
    s=""
    h=hashlib.md5()
    i=0
    for x in range(128):
        if x<92:
            h.update(array[i:i+38])
            i=i+38
        else:
            h.update(array[i:i+39])
            i=i+39
        temp=h.hexdigest()
        s=s+temp[0]
    
    return s
    
    
def detbuckets(s,bands,buckets):
    
    rows=int(128/bands)
    
    belongsto=[]
    for i in range(bands):
        x=hash(s[i*rows:i*rows+rows])%buckets
        belongsto.append((x,i))
        
    return belongsto
    
    
def determineV(x):
    
    img=[]
    img_dim=[]
    
    for (k,v) in x:
        temp=[]
        img.append(k)
        for i in range(len(v)):
            temp.append(v[i])
        img_dim.append(temp)
        
    img_dim=np.array(img_dim)
    
    mu, std = np.mean(img_dim, axis=0), np.std(img_dim, axis=0)
    for i in range(len(std)):
        if std[i]==0:
            std[i]=1
    
    img_dim_zs = (img_dim - mu) / std

    U, s, Vh = linalg.svd(img_dim_zs, full_matrices=0, compute_uv=1)
    
    Vh=np.transpose(Vh)
    
    return Vh

def pca(x,v):
    
    y=np.matmul(x,v)
    return y
    
    
def calcdistance(array1, array2):
    
    tot=0
    
    for i in range(len(array1[0])):
        tot=(array1[0][i]-array2[0][i])**2+tot
        
    distance=tot**(0.5)
    
    return distance
       
    
def DiffResolutionCode(convrdd,factor,bands,buckets):
    
    
    #reducing 500x500 by factor*factor
    convrdd=convrdd.map(lambda x: (x[0],breakimage(x[1],factor,1))) #this will call breakimage with takemean=1
    #print(convrdd.collect())
    
    
    #I have created separate rdds as earlier it was not mentioned whether we have to create separate or same rdd for each
    
    #calculate rowdiff and coldiff intensities
    rowdiff=convrdd.map(lambda x: (x[0],diff(x[1])))
    #print(rowdiff.collect())
    
    coldiff=convrdd.map(lambda x: (x[0],diff(x[1],0)))
    #print(coldiff.collect())
    
    #flattening and merging rowdiff and coldiff
    rowdiff=rowdiff.map(lambda x: (x[0],x[1].flatten()))
    coldiff=coldiff.map(lambda x: (x[0],x[1].flatten()))
    
    features=rowdiff.join(coldiff).map(lambda x: (x[0],np.hstack((x[1][0],x[1][1]))))
    #print(features.collect())
    
    #print sample values
    print("Feature vectors for given images in 2(f): \n")
    samplerdd2=features.filter(lambda x: x[0] in ["3677454_2025195.zip-1","3677454_2025195.zip-18"])
    print(samplerdd2.collect())
    print("\n\n")
    
    #determining signatures for each feature vector
    signature=features.map(lambda x: (x[0],hashit(x[1]),x[1]))
    #print(signature.collect())
    
    #categorizing each image into its (bucket,band) tuple
    rdd2=signature.map(lambda x: (x[0],detbuckets(x[1],bands,buckets),x[2])).flatMap(lambda x: [((a,b),(x[0],x[2])) for (a,b) in x[1]]).groupByKey().mapValues(list)
    #print(rdd2.collect())
    
    

    print("Similar Images for 3677454_2025195.zip-0 in 3(b): \n")
    sample1=rdd2.map(lambda x: (x[0],[w[0] for w in x[1]])).filter(lambda x: "3677454_2025195.zip-0" in x[1]).flatMap(lambda x: [w for w in x[1] if w!="3677454_2025195.zip-0"]).distinct()
    print(sample1.collect())
    print("\n\n")
    
    print("Similar Images for 3677454_2025195.zip-1 in 3(b): \n")
    sample2=rdd2.map(lambda x: (x[0],[w[0] for w in x[1]])).filter(lambda x: "3677454_2025195.zip-1" in x[1]).flatMap(lambda x: [w for w in x[1] if w!="3677454_2025195.zip-1"]).distinct()
    print(sample2.collect())
    print("\n\n")
    
    print("Similar Images for 3677454_2025195.zip-18 in 3(b): \n")
    sample3=rdd2.map(lambda x: (x[0],[w[0] for w in x[1]])).filter(lambda x: "3677454_2025195.zip-18" in x[1]).flatMap(lambda x: [w for w in x[1] if w!="3677454_2025195.zip-18"]).distinct()
    print(sample3.collect())
    print("\n\n")
    
    print("Similar Images for 3677454_2025195.zip-19 in 3(b): \n")
    sample4=rdd2.map(lambda x: (x[0],[w[0] for w in x[1]])).filter(lambda x: "3677454_2025195.zip-19" in x[1]).flatMap(lambda x: [w for w in x[1] if w!="3677454_2025195.zip-19"]).distinct()
    print(sample4.collect())
    print("\n\n")
    
    #determining V matrix
    nsample=features.map(lambda x: x[0]).takeSample(False,10)
    Vh=features.filter(lambda x: x[0] in nsample).map(lambda x: (1,x)).groupByKey().mapValues(list).map(lambda x: determineV(x[1]))
    V=sc.broadcast(Vh.collect())
    
    #Reducing 4900 dimensions to 10 dimensions for each image
    pca_reduced_features=features.map(lambda x: (x[0],pca(x[1],V.value)))
    #print(pca_reduced_features.collect())
    
    print("Euclidean Distance for images similar to 3677454_2025195.zip-1 in 3(c): \n")
    z1=sample2.collect()
    ziplist1=pca_reduced_features.filter(lambda x: x[0]=="3677454_2025195.zip-1")
    imgs2=pca_reduced_features.filter(lambda x: x[0] in z1)
    eucledian_distance1=ziplist1.cartesian(imgs2)
    eucledian_distance1=eucledian_distance1.map(lambda x: (calcdistance(x[0][1],x[1][1]),(x[0][0],x[1][0]))).sortByKey(ascending=True)
    print(eucledian_distance1.collect())
    print("\n\n")
     
    
    print("Euclidean Distance for images similar to 3677454_2025195.zip-18 in 3(c): \n")
    z2=sample3.collect()
    ziplist2=pca_reduced_features.filter(lambda x: x[0]=="3677454_2025195.zip-18")
    imgs2=pca_reduced_features.filter(lambda x: x[0] in z2)
    eucledian_distance2=ziplist2.cartesian(imgs2)
    eucledian_distance2=eucledian_distance2.map(lambda x: (calcdistance(x[0][1],x[1][1]),(x[0][0],x[1][0]))).sortByKey(ascending=True)
    print(eucledian_distance2.collect())
    print("\n\n")
    
    
    
if __name__ == "__main__":
    
    conf= SparkConf()
    conf.setAppName("Assignment 2")
    sc = SparkContext(conf=conf)
    
    #Reading the files
    rdd=sc.binaryFiles("hdfs:/data/large_sample")
    
    
    #Filtering the images
    rdd=rdd.map(lambda x: ((x[0].split("/")),x[1])).map(lambda x: (x[0][-1],x[1]))
    
    
    #getting the arrays corresponding to each image
    rdd=rdd.map(lambda x: (x[0],getOrthoTif(x[1])))
    #print(rdd.collect())
    
    
    #breaking the image into 25 evenly sized subimages
    rdd=rdd.map(lambda x: (x[0],breakimage(x[1],500)))
    #print(rdd.collect())
    
    #creating a final rdd for (imagename, array)
    finalrdd=rdd.flatMap(lambda x: [(x[0]+"-"+str(i),x[1][i]) for i in range(25)])
    #print(finalrdd.collect())

    
    #print sample values
    samplerdd=finalrdd.filter(lambda x: x[0] in ["3677454_2025195.zip-0","3677454_2025195.zip-1","3677454_2025195.zip-18","3677454_2025195.zip-19"]).map(lambda x: x[1][0][0])
    
    print("r, g, b, x values for the pixel (0,0) for the given images in 1(a): \n")
    print(samplerdd.collect())
    print("\n\n")
    
    
    #converting rgbx into intensity values
    convrdd=finalrdd.map(lambda x: (x[0],intensity(x[1])))
    #print(convrdd.collect())
    
    #code for different resolutions
    print("Output for factor=10")#3(c)
    DiffResolutionCode(convrdd,factor=10,bands=20,buckets=1040)
    print("\n\n\n")
    
    
    print("Output for factor=5")#3(d) 
    DiffResolutionCode(convrdd,factor=5,bands=17,buckets=1040)
    