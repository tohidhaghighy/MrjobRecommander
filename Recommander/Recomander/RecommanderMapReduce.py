from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np
import logging

class MRJoin(MRJob):

    def mapper_first(self, key, line):
        a=[]
        if len(line)>40:
            a = line.split("|")
            yield int(a[0]),("I",a[0],a[5],a[6],a[7],a[8],a[9],a[10],a[11],a[12],a[13],a[14],a[15],a[16],a[17],a[18],a[19],a[20],a[21],a[22],a[23])
        else:
            a=line.split("\t")
            if int(a[0]) < 50:
                yield int(a[1]),("U",a[0],a[1],a[2])

    def reducer_first(self, key, values):
        itemlist=[]
        userlist=[]
        for a in values:
            if a[0]=="I":
                itemlist.append(a)
            elif a[0]=="U":
                userlist.append(a)
    
        for item in itemlist:
            for user in userlist:
                yield int(user[1]),(user[1],item[1],user[3],item[2],item[3],item[4],item[5],item[6],item[7],item[8],item[9],item[10],item[11],item[12],item[13],item[14],item[15],item[16],item[17],item[18],item[19],item[20])

        
    def reducer_second(self, key, values):
        userdic = {"key":key,"itemid":0,"rating":0,"unknown":0,"action":0,"adventure":0,"animation":0,"childeren":0,"comedy":0,"crime":0,"documentry":0,"drama":0,"fantasy":0,"filmnoir":0,"horror":0,"musical":0,"mystery":0,"romance":0,"sci-fi":0,"thriller":0,"war":0,"western":0,}
        ganrecounter = {"key":key,"itemid":0,"rating":0,"unknown":0,"action":0,"adventure":0,"animation":0,"childeren":0,"comedy":0,"crime":0,"documentry":0,"drama":0,"fantasy":0,"filmnoir":0,"horror":0,"musical":0,"mystery":0,"romance":0,"sci-fi":0,"thriller":0,"war":0,"western":0,}
        counter=0
        for val in values:
            counter += 1
            userdic["itemid"] = val[1]
            userdic["rating"] += int(val[2])
            if int(val[3])!=0:
                ganrecounter["unknown"]+=1
            userdic["unknown"] += (int(val[3]) * int(val[2]))
            if int(val[4])!=0:
                ganrecounter["action"]+=1
            userdic["action"] += (int(val[4]) * int(val[2]))
            if int(val[5])!=0:
                ganrecounter["adventure"]+=1
            userdic["adventure"] += (int(val[5]) * int(val[2]))
            if int(val[6])!=0:
                ganrecounter["animation"]+=1
            userdic["animation"] += (int(val[6]) * int(val[2]))
            if int(val[7])!=0:
                ganrecounter["childeren"]+=1
            userdic["childeren"] += (int(val[7]) * int(val[2]))
            if int(val[8])!=0:
                ganrecounter["comedy"]+=1
            userdic["comedy"] += (int(val[8]) * int(val[2]))
            if int(val[9])!=0:
                ganrecounter["crime"]+=1
            userdic["crime"] += (int(val[9]) * int(val[2]))
            if int(val[10])!=0:
                ganrecounter["documentry"]+=1
            userdic["documentry"] += (int(val[10]) * int(val[2]))
            if int(val[11])!=0:
                ganrecounter["drama"]+=1
            userdic["drama"] += (int(val[11]) * int(val[2]))
            if int(val[12])!=0:
                ganrecounter["fantasy"]+=1
            userdic["fantasy"] += (int(val[12]) * int(val[2]))
            if int(val[13])!=0:
                ganrecounter["filmnoir"]+=1
            userdic["filmnoir"] += (int(val[13]) * int(val[2]))
            if int(val[14])!=0:
                ganrecounter["horror"]+=1
            userdic["horror"] += (int(val[14]) * int(val[2]))
            if int(val[15])!=0:
                ganrecounter["musical"]+=1
            userdic["musical"] += (int(val[15]) * int(val[2]))
            if int(val[16])!=0:
                ganrecounter["mystery"]+=1
            userdic["mystery"] += (int(val[16]) * int(val[2]))
            if int(val[17])!=0:
                ganrecounter["romance"]+=1
            userdic["romance"] += (int(val[17]) * int(val[2]))
            if int(val[18])!=0:
                ganrecounter["sci-fi"]+=1
            userdic["sci-fi"] += (int(val[18]) * int(val[2]))
            if int(val[19])!=0:
                ganrecounter["thriller"]+=1
            userdic["thriller"] += (int(val[19]) * int(val[2]))
            if int(val[20])!=0:
                ganrecounter["war"]+=1
            userdic["war"] += (int(val[20]) * int(val[2]))
            if int(val[21])!=0:
                ganrecounter["western"]+=1
            userdic["western"] += (int(val[21]) * int(val[2]))
        
        userdic["rating"]/=counter

        ganrelist=""
        if ganrecounter["unknown"]==0:
            userdic["unknown"]=0
        else:
            userdic["unknown"]/=ganrecounter["unknown"]
            if userdic["unknown"]>=3:
                ganrelist+="unknown - "
        
        if ganrecounter["action"]==0:
                userdic["action"]=0
        else:
            userdic["action"]/=ganrecounter["action"]
            if userdic["action"]>=3:
                ganrelist+="action - "

        if ganrecounter["adventure"]==0:
                userdic["adventure"]=0
        else:
            userdic["adventure"]/=ganrecounter["adventure"]
            if userdic["adventure"]>=3:
                ganrelist+="adventure - "

        if ganrecounter["animation"]==0:
                userdic["animation"]=0
        else:
            userdic["animation"]/=ganrecounter["animation"]
            if userdic["animation"]>=3:
                ganrelist+="animation - "

        if ganrecounter["childeren"]==0:
                userdic["childeren"]=0
        else:
            userdic["childeren"]/=ganrecounter["childeren"]
            if userdic["childeren"]>=3:
                ganrelist+="childeren - "

        if ganrecounter["comedy"]==0:
                userdic["comedy"]=0
        else:
            userdic["comedy"]/=ganrecounter["comedy"]
            if userdic["comedy"]>=3:
                ganrelist+="comedy - "

        if ganrecounter["crime"]==0:
                userdic["crime"]=0
        else:
            userdic["crime"]/=ganrecounter["crime"]
            if userdic["crime"]>=3:
                ganrelist+="crime - "

        if ganrecounter["documentry"]==0:
                userdic["documentry"]=0
        else:
            userdic["documentry"]/=ganrecounter["documentry"]
            if userdic["documentry"]>=3:
                ganrelist+="documentry - "

        if ganrecounter["drama"]==0:
                userdic["drama"]=0
        else:
            userdic["drama"]/=ganrecounter["drama"]
            if userdic["drama"]>=3:
                ganrelist+="drama - "


        if ganrecounter["fantasy"]==0:
                userdic["fantasy"]=0
        else:
            userdic["fantasy"]/=ganrecounter["fantasy"]
            if userdic["fantasy"]>=3:
                ganrelist+="fantasy - "

        if ganrecounter["filmnoir"]==0:
                userdic["filmnoir"]=0
        else:
            userdic["filmnoir"]/=ganrecounter["filmnoir"]
            if userdic["filmnoir"]>=3:
                ganrelist+="filmnoir - "

        if ganrecounter["horror"]==0:
                userdic["horror"]=0
        else:
            userdic["horror"]/=ganrecounter["horror"]
            if userdic["horror"]>=3:
                ganrelist+="horror - "

        if ganrecounter["musical"]==0:
                userdic["musical"]=0
        else:
            userdic["musical"]/=ganrecounter["musical"]
            if userdic["musical"]>=3:
                ganrelist+="musical - "

        if ganrecounter["mystery"]==0:
                userdic["mystery"]=0
        else:
            userdic["mystery"]/=ganrecounter["mystery"]
            if userdic["mystery"]>=3:
                ganrelist+="mystery - "

        if ganrecounter["romance"]==0:
                userdic["romance"]=0
        else:
            userdic["romance"]/=ganrecounter["romance"]
            if userdic["romance"]>=3:
                ganrelist+="romance - "

        if ganrecounter["sci-fi"]==0:
                userdic["sci-fi"]=0
        else:
            userdic["sci-fi"]/=ganrecounter["sci-fi"]
            if userdic["sci-fi"]>=3:
                ganrelist+="sci-fi - "

        if ganrecounter["thriller"]==0:
                userdic["thriller"]=0
        else:
            userdic["thriller"]/=ganrecounter["thriller"]
            if userdic["thriller"]>=3:
                ganrelist+="thriller - "

        if ganrecounter["war"]==0:
                userdic["war"]=0
        else:
            userdic["war"]/=ganrecounter["war"]
            if userdic["war"]>=3:
                ganrelist+="war - "

        if ganrecounter["western"]==0:
                userdic["western"]=0
        else:
            userdic["western"]/=ganrecounter["western"]
            if userdic["western"]>=3:
                ganrelist+="western - "
        

        yield key , ganrelist


      


    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_first,
                reducer=self.reducer_first
            ),
            MRStep(
                reducer=self.reducer_second
            )
        ]
if __name__ == '__main__':
    MRJoin.run()