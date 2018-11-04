# Databricks notebook source


#Pontifícia Universidade Católica de Minas Gerais
#Ciências de Dados e Big Data / 4   
#Aluno: Lucas Xavier da Silva


#1. Faça a importação do arquivo TXT 
trainDF = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/train.csv")

# COMMAND ----------

#2. Apresente o esquema da estrutura importada
trainDF.printSchema()

# COMMAND ----------

#3. Verifique a quantidade de linhas da estrutura importada
trainDF.count()

# COMMAND ----------

#4. Faça uma análise estatística descritiva básica(média, desvio padrão, quantidade, min, max ) para a coluna Purchase (transforme a coluna purchase para o tipo de dados inteiro)
from pyspark.sql.functions import expr
trainDF.withColumn("Purchase", expr("CAST(Purchase AS INTEGER)"))
trainDF.select('Purchase').describe().show()

# COMMAND ----------

#5. Verifique o número de produtos distintos contidos nesta base de dados
trainDF.select('Product_ID').distinct().count()

# COMMAND ----------

#6. Verifique o número de pessoas que compraram os produtos por idade cruzados por gênero
trainDF.crosstab('Age', 'Gender').show()

# COMMAND ----------

#7. Crie outro data frame em que o valor da compras(purchase) foi maior que 20.000 e depois exiba o número de linhas: 
trainDF20000 = trainDF.filter(trainDF.Purchase > 20000)
trainDF20000.count()

# COMMAND ----------

#8. Crie outro data frame em que o valor da compras(purchase) foi maior que 20.000 e menor que 45000 somente para as mulheres. Depois exiba o número de linhas: 
trainDF45000 = trainDF.filter((trainDF.Purchase > 20000) & (trainDF.Purchase < 45000) & (trainDF.Gender=="F"))
trainDF45000.count()

# COMMAND ----------

#9. Encontre a média de compras (purchase) de cada uma das faixas de idade: 
trainDF.groupBy("Age").agg({'Purchase': 'avg'}).orderBy("Age").show()

# COMMAND ----------

#10. Encontre a média de compras(purchase) de cada um por genero: 
trainDF.groupBy("Gender").agg({'Purchase': 'avg'}).orderBy("Gender").show()

# COMMAND ----------

#11. Faça uma simulação gerando um novo dataframe com a média de compras por idade aumentada em 10%:
novoDF = trainDF.groupBy("Age").agg({'Purchase': 'avg'})
novoDF.withColumn("Pur_10_percent", novoDF["avg(Purchase)"] *1.1).orderBy("Age").show()
