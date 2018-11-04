-- Databricks notebook source
/*

Pontifícia Universidade Católica de Minas Gerais
Ciências de Dados e Big Data / 4   
Aluno: Lucas Xavier da Silva

*/

/* 1. Faça a importação do arquivo TXT */
DROP TABLE IF EXISTS train;

CREATE TEMPORARY TABLE train
  USING csv
  OPTIONS (path "/FileStore/tables/train.csv", header "true", mode "FAILFAST")

-- COMMAND ----------

/* 2. Apresente o esquema da estrutura importada */
describe train

-- COMMAND ----------

/* 3. Verifique a quantidade de linhas da estrutura importada */
select count(*) from train

-- COMMAND ----------

/* 4. Faça uma análise estatística descritiva básica(média, desvio padrão, quantidade, min, max ) para a coluna Purchase (transforme a coluna purchase para o tipo de dados inteiro) */
select count(CAST(Purchase AS Int)) AS count,AVG(CAST(Purchase AS Int)) AS mean,STDDEV_SAMP(CAST(Purchase AS Int)) AS stddev,min(CAST(Purchase AS Int)) AS min,max(CAST(Purchase AS Int)) AS max from train

-- COMMAND ----------

/* 5. Verifique o número de produtos distintos contidos nesta base de dados */
select count(distinct Product_ID) as total from train

-- COMMAND ----------

/* 6. Verifique o número de pessoas que compraram os produtos por idade cruzados por gênero */
select Age,Gender, count(*) as total from train group by Gender,Age order by Gender,Age

-- COMMAND ----------

/* 7. Crie outro data frame em que o valor da compras(purchase) foi maior que 20.000 e depois exiba o número de linhas:  */
DROP VIEW IF EXISTS train_20000;
CREATE TEMPORARY VIEW train_20000 AS (select * from train where CAST(Purchase AS Int)>20000);
select count(*) from train_20000

-- COMMAND ----------

/* 8. Crie outro data frame em que o valor da compras(purchase) foi maior que 20.000 e menor que 45000 somente para as mulheres. Depois exiba o número de linhas:  */
DROP VIEW IF EXISTS train_20000_4500_f;
CREATE TEMPORARY VIEW train_20000_4500_f AS (select * from train where CAST(Purchase AS Int)>20000 and  CAST(Purchase AS Int)<45000 and Gender="F");
select count(*) from train_20000_4500_f

-- COMMAND ----------

/* 9. Encontre a média de compras (purchase) de cada uma das faixas de idade:  */ 
select Age,AVG(CAST(Purchase AS Int)) as media from train group by (Age) order by Age asc

-- COMMAND ----------

/* 10. Encontre a média de compras(purchase) de cada um por genero: */
select Gender,AVG(CAST(Purchase AS Int)) as medi from train group by (Gender) order by Gender asc

-- COMMAND ----------

/* 11. Faça uma simulação gerando um novo dataframe com a média de compras por idade aumentada em 10%: */
select Age,AVG(CAST(Purchase AS Int)) as media,(AVG(CAST(Purchase AS Int))*1.1) as media_mais_10 from train group by (Age) order by Age asc
