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

2. Faça o clone desse repositório no nó master.
```
$ git clone https://github.com/topicos-sistemas-distribuidos/words.git
```

3. No pom.xml, na linha, <mainClass> br.ufc.great.es.tsd.mapreduce.words.WordCount </mainClass> indique a feature (WordCount, WordLegth ou WordAverage) que será compilada.

4. Execute um maven clean
```
$ mvn clean
```

5. Execute um maven compile
```
$mvn compile
```

6. Execute um maven package
```
$mvn package
```

7. Execute o script de acordo com a feature compilada
```
$./my-wordcount.sh -count
$./my-wordlength.sh -length
$./my-wordaverage.sh -average
```

Obs: Os scripts precisam de permissão para executar. Por exemplo: chmod +x my-wordcount.sh

Para a execução do my-wordcount.sh - O output deverá ser uma lista contendo palavra e quantas vezes ela apareceu no texto.

Para a execução do my-wordlength.sh - O output deverá ser uma lista com palavra e o seu tamanho.

Para a execução do my-wordavarage.sh - O output deverá ser a média do tamanho das palavras.  
