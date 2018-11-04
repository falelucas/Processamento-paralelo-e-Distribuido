import mincemeat
import glob
import csv

#alterei diretorio
text_files = glob.glob('data\\vendas\\*')

#formatacao furada
def file_contents(file_name):
    f = open(file_name)
    try:
	    return f.read()
    finally:
	    f.close()

#formatacao furada
def mapfn(k,v):
    print 'map '+k
    for line in v.splitlines():
        if k == 'data\\vendas\\2.2-vendas.csv':
            yield line.split(';')[0], 'Vendas'+':'+line.split(';')[5]
        if k == "data\\vendas\\2.2-filiais.csv":
            yield line.split(';')[0], 'Filial'+ ':'+line.split(';')[1]
				
#def reducefn(k,v):
#    print 'redue '+k
#    return v

def reducefn(k,v):
    print 'redue '+k
    total = 0
    nomeFilial = ""
    for index, item in enumerate(v):
        if item.split(':')[0]== 'Vendas':
            total = int(item.split(":")[1])+total
        if item.split(":")[0]=='Filial':
            nomeFilial = item.split(":")[1]
    L = list()
    L.append(nomeFilial+" , "+str(total))
    return L

source = dict((file_name,file_contents(file_name))for file_name in text_files)


s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="abc")

w = csv.writer(open("result-join.csv","w"))
#w = csv.writer(open("result-join-reducesimples.csv","w"))
for k, v in results.items():
    #w.writerow([k,v])
    w.writerow([k,str(v).replace("[","").replace("]","").replace("'","").replace(' ','')])





