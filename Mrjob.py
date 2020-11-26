from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np
import logging

class MRJoin(MRJob):

    S_Line=6;
    def mapper_first(self, key, line):
        a=[]
        a=line.split(' ')
        try:
            total = np.float(a[2])
        except TypeError:
            #print("skipping line with value", a[2])
            pass
        else:
            if(a[0]=='R'):
                yield int(a[1]),(a[0],a[2],a[3])
            else:
                for i in range(6):
                    yield i+1,(a[0],a[2],a[3])
                      

    def reducer_first(self, key, values):
        arry1=[]
        arry2=[]
        for matrix,first,second in values:
            if  matrix=="R":
                arry1.append({"key":matrix,"value1":first,"value2":second})
            if matrix=="S":
                arry2.append({"key":matrix,"value1":first,"value2":second})
                

        for l in arry1:
            for m in arry2:
                if(l["value2"] < m["value1"]):
                    yield (l["value2"],m["value1"]),(l["value1"],l["value2"],m["value1"],m["value2"])
                

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_first,
                reducer=self.reducer_first
            ),
            # MRStep(
            #     reducer=self.reducer_second
            # )
        ]
if __name__ == '__main__':
    MRJoin.run()