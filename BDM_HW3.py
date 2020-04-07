#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext

def mapper(partition, records):
    if partition==0:
        next(records)
    import csv
    reader = csv.reader(records)
    for r in reader:
        # extract product, company and year information
        yield (r[1], int(r[0][:4])), r[7].lower()

if __name__=='__main__':
    sc = SparkContext()
    Complaint = 'complaints.csv'
    data = sc.textFile(Complaint, use_unicode=True).mapPartitionsWithIndex(mapper)
    
    year_complaint = data.mapValues(lambda x: 1).reduceByKey(lambda x,y: x+y)
    
    company_number = data.mapValues(lambda x: [x]).reduceByKey(lambda x,y: x+y).mapValues(lambda x: len(set(x)))
    
    company_complaints = data                                 .map(lambda x: (x, 1))                                 .reduceByKey(lambda x, y: x+y)                                .map(lambda x: (x[0][0],x[1]))                                 .reduceByKey(lambda x, y: max(x,y))
    outcome = year_complaint.join(company_number).join(company_complaints).mapValues(lambda x: (x[0][0],x[0][1],round(100*x[1]/x[0][0])))                                         .sortByKey(ascending=True)                                         .saveAsTextFile('output')


# In[ ]:




