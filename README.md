# words
Wordcount, Wordlength and Wordaverage using hadoop

Features
---
1. Faz a contagem de palavras de arquivos texto de uma pasta
2. Para os textos analisados de arquivos texto, mostra a palavra e o tamanho da palavra
3. Para os textos analisados de arquivos texto, mostra a media do tamanho das palavras

Roteiro de execução
---
Faça a configuração do Cluster do Hadoop com 3 nós usando containers Docker disponível em https://github.com/topicos-sistemas-distribuidos/hadoop-cluster-docker

1. Vá até o nó master do hadoop. 

Obs: Inicie o hdfs e o yarn. É preciso garantir que o nó master tenha instalado o git, o maven e um editor de texto.  

2. No pom.xml em <mainClass>br.ufc.great.es.tsd.mapreduce.words.WordCount</mainClass> indique a feature (WordCount, Wordlegth, ou WordAverage) que será compilada.

3. Execute um maven clean
```
$ mvn clean
```

4. Execute um maven compile
```
$mvn compile
```

5. Execute um maven package
```
$mvn package
```

6. Execute o script de acordo com a feature compilada
```
$./my-wordcount.sh -count
$./my-wordlength.sh -length
$./my-wordaverage.sh -average
```

Para a execução do my-wordcount.sh - O output deverá ser uma lista contendo palavra e quantas vezes ela apareceu no texto.

Para a execução do my-wordlength.sh - O output deverá ser uma lista com palavra e o seu tamanho.

Para a execução do my-wordavarage.sh - O output deverá ser a média do tamanho das palavras.  
