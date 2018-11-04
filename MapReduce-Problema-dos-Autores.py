import mincemeat
import glob
import csv
import string

text_files = glob.glob('data\\problema-dos-autores\\*');

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

def mapfn(k,v):
	print 'map '+k
	
	for line in v.splitlines():
		title = line.split(":::")
		for author in title[1].split("::"):
			yield author, str(title[2])

def reducefn(k,v):
	print 'reduce :'+k+ "on v:"+str(v)
	from stopwords import allStopWords
	import string
	palavrasDoAutor = []
	v = str(str(v).lower().translate(None, string.punctuation))
	palavrasDoAutor = {}
	for item in v.split():
		if(item not in allStopWords):
			if(palavrasDoAutor.get(str(item))):
				palavrasDoAutor[str(item)]=palavrasDoAutor[str(item)]+1
			else:
				palavrasDoAutor[str(item)] = 1
	  
	aret = []
	for x, y in palavrasDoAutor.items():
	  aret.append(str(x)+": "+str(y))

	return ", ".join(aret)

source = dict((file_name,file_contents(file_name))for file_name in text_files)


s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password='123')

w = csv.writer(open("tb_061018.csv","w"))
for k, v in results.items():
    w.writerow([k,v])
	
	
"""
5) Responda quais sao as duas palavras que mais acontecem para os seguintes autores:
a- Grzerorz Rozenberg : 

  				systems	10
  				grammars	9

 Todas: directions: 1, abstract: 1, predicates: 1, experiments: 1, agents: 1, strategy: 1, state: 1, program: 3, parallelism: 1, approach: 1, transformation: 2, synthesis: 1, development: 1, abstraction: 1, transforming: 2, comprehensive: 1, logic: 2, observers: 1, infinite: 1, using: 2, inductive: 1, a: 1, processes: 1, programs: 2, developing: 1, eureka: 1, future: 1, verification: 1, sets: 1, definitions: 1, lambda: 1


b- Philip S. Yu

 			web	7
 			a	7

 Todas: infominer: 1, similarity: 1, automated: 1, dynamic: 4, stree: 1, report: 1, concurrency: 1, query: 1, disk: 1, issues: 1, web: 7, microarray: 1, distributed: 6, environment: 2, topical: 1, systems: 4, discovering: 1, clusters: 1, advanced: 1, parallelizable: 1, hit: 1, combination: 1, rules: 1, indexing: 1, scalable: 2, dat: 1, balancing: 2, optimal: 1, clustered: 1, world: 2, multidimensional: 1, execution: 1, association: 1, resource: 1, approach: 2, server: 1, large: 2, queries: 1, versioning: 2, crawler: 1, parity: 1, set: 1, webbased: 1, generation: 1, searching: 1, second: 1, design: 2, recommendation: 1, dynamics: 1, index: 3, pattern: 2, multimedia: 1, sites: 1, access: 1, buffer: 3, state: 1, sequential: 1, international: 1, increasing: 1, evaluation: 1, discovery: 1, crawling: 1, processing: 2, analyzer: 1, prediction: 1, heterogeneous: 1, objects: 1, strategies: 1, path: 1, dclusters: 1, fault: 1, study: 1, tolerant: 1, periodic: 1, throughput: 1, grouping: 1, load: 4, tasks: 1, probability: 1, characterization: 1, servers: 1, predicates: 1, unexpected: 1, learning: 1, estimators: 1, table: 1, multijoin: 1, ecommerce: 1, weightedsequences: 1, management: 1, dna: 1, analytic: 1, arbitrary: 1, system: 3, relations: 1, response: 1, workshop: 1, time: 1, flexible: 1, consumptionbased: 1, engine: 1, a: 7, mining: 2, sharing: 4, subspace: 1, traversal: 1, wireless: 1, task: 1, coherency: 1, capturing: 1, wide: 2, partitioning: 1, handling: 1, join: 1, effective: 1, arrays: 1, raid5: 1, minimize: 1, optimization: 1, policies: 3, site: 1, partitioned: 1, control: 2, computing: 1, lock: 1, personalization: 1, optimizing: 1, comparative: 1, scheduling: 1, information: 2, improving: 1, efficient: 3, multisystem: 2, write: 1, geographically: 1, online: 1, performance: 5, finite: 1, intelligent: 2, profile: 1, farm: 1, assignment: 2, correlation: 1, allocation: 1, concurrent: 1, webserver: 1, portal: 1, architectures: 2, tabsum: 1, robust: 1, data: 7, parallel: 2, structure: 1, summarization: 1, transaction: 2, coupling: 3, surprising: 1, database: 3, mobile: 1, average: 1, analysis: 4, patterns: 2, competitors: 1, broadcasting: 1, algorithms: 1, databases: 1, caching: 1, multiprocessor: 1


"""
	