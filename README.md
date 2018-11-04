# Processamento-paralelo-e-Distibuido
Pós Graduação em Ciências de Dados e Big Data - Soluções para processamento paralelo e Distribuido


## 1 - (MapReduce-Contagem-de-palavras.py)
Um conjunto de comentários sobre os produtos da sua empresa foi disponibilizado (textos em
inglês). Todos eles são comentários positivos sobres os produtos. O seu gerente solicitou que
você prepare uma solução que possa atender os seguinte requisitos:

	I. Armazenar os textos e extrair  palavras chaves que são mais citadas pelos clientes;
	II. Sua companhia possui 4 milhões de clientes e há uma interação com estes clientes
	pelo menos uma vez por mês;
	III. Para cada mês deve haver uma atualização nas palavras ;
	IV. Em média um cliente interage com a companhia 1 vez por mês; Cada interação gera
	um arquivo texto a partir do sistema fonte de informação que armazena este dado;
	V. Algumas palavras deve ser descartadas: 'by', 'above', 'all', 'none', 'nobody'......

Sua missão é fazer uma prova de conceito para o seu gerente. Ele ouviu falar de
processamento paralelo e leu um artigo na 'Computer magazine' falando sobre o movimento
NoSQL: quer entender no cenário da empresa como isto poderia funcionar. Ele já solicitou
um extração de 1000 arquivos sobre os comentário dos clientes. Em uma reunião foi definido
que você deve entregar um exemplo funcionando amanhã

Você deve então:
	a- Usar uma implementação simples e rápida;
	b- Permitir que isto seja executada em 50 máquinas na empresa;
	c- No final gerar um arquivo com a contagem das palavras;


## 1 - (MapReduce-SQL.py)	
Neste caso vamos implementar um junção e agrupamento de duas tabelas que estão
representadas por dois arquivos Vendas e Filiais.

Neste caso o que se deseja é fazer um junção dos dois arquivos para que seja apresentado
um resultado que seria: Código da filial ( arquivo Filial - campo1), descrição da filial
( arquivo Filial - campo2) e total de itens pedidos ( arquivo Filial – campo6);
Na verdade seria um SQL com join e um group by:
Select cod_filial, des_filial, sum(qtd_item) as total
from vendas inner join filial on(filial.cod_filial = vendas.cod_filial)
group by cod_filial, des_filial

	
## 3 - (MapReduce-Problema-dos-Autores.py)
Cada arquivo no diretorio data/problema-dos-autores diretório possui a seguinte estrutura:

journals/cl/SantoNR90:::Michele Di Santo::Libero Nigro::Wilma Russo:::Programmer-
Defined Control Abstractions in Modula-2.

Cada uma das linhas pode ser entendida como:
Informação bibliográfica;
autores separados por '::';
Título da obra
paper-id:::author1::author2::.... ::authorN:::title


1) A tarefa é fazer o cálculo de quantas vezes cada termo (no título da obra) acontece por autor;
2) Por exemplo para o autor 'Alberto Pettorossi' os seguinte termos acontecem (considerando
todos os documentos): program:3, transformation:2, transforming:2, using:2, programs:2,
and logic:2.
3) Observe os seguintes itens:

O separador de campos é “:::” e separador de autores é “::”;
Cada autor pode ter escrito múltiplas obras, que por sua vez podem estar em vários
arquivos;
Existe um lista de palavras que não serão consideradas. Utilize o arquivo
stopword.py do exercício prático: exclua todas estas palavras para os autores;
Se possível faça a exclusão de pontuações também, de forma que a palavra logic e
logic. sejam tratadas como se fossem uma única palavra;

4) Entregue a implementação do seu grupo;
5) Responda quais são as duas palavras que mais acontecem para os seguintes autores:
a- “Grzerorz Rozenberg”
b- “Philip S. Yu”



## Trabalho Final - /trabalho-final

No arquivo data/trabalho-final/train.csv temos dados de promoções e vendas realizadas em um rede de lojas
Este é um trabalho guida para cada um dos itens abaixo solicitados você deve apresentar o
código spark (SQL, PySpark, Scala) utilizado.
Colunas contidas no arquivo:

1. Faça a importação do arquivo TXT
2. Apresente o esquema da estrutura importada
3. Verifique a quantidade de linhas da estrutura importada
4. Faça uma análise estatística descritiva básica(média, desvio padrão, quantidade, min,
max ) para a coluna Purchase (transforme a coluna purchase para o tipo de dados
inteiro)
5. Verifique o número de produtos distintos contidos nesta base de dados
6. Verifique o número de pessoas que compraram os produtos por idade cruzados por
gênero
7. Crie outro data frame em que o valor da compras(purchase) foi maior que 20.000 e
depois exiba o número de linhas:
8. Crie outro data frame em que o valor da compras(purchase) foi maior que 20.000 e
menor que 45000 somente para as mulheres. Depois exiba o número de linhas:
9. Encontre a média de compras (purchase) de cada uma das faixas de idade:
10. Encontre a média de compras(purchase) de cada um por genero:
11. Faça uma simulação gerando um novo dataframe com a média de compras por idade
aumentada em 10%: