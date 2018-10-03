import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Analisador {

	
  // Args = path/to/text0.txt path/to/text1.txt
  def main(args: Array[String]) {

	//val t1 = System.nanoTime

    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Contagem de Palavra"))

    println("TEXT1")

    // read first text file and split into lines
    val lines1 = sc.textFile(args(0))

    // TODO: contar palavras do texto 1 e imprimir as 5 palavras com as maiores ocorrencias (ordem DECRESCENTE)
    // imprimir na cada linha: "palavra=numero"
    val counts1 = lines1.flatMap(line => line.replaceAll("[,.!?:;]","").toLowerCase().split(" "))
                 .filter(word => word.length() > 3)
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
    
    val output1 = counts1.sortBy(_._2, false).take(5)
    for ((word, count) <- output1) {
        println(word + "=" + count)
	}
	
	println("TEXT2")

    // read second text file and split each document into words
    val lines2 = sc.textFile(args(1))

    // TODO: contar palavras do texto 2 e imprimir as 5 palavras com as maiores ocorrencias (ordem DECRESCENTE)
    // imprimir na cada linha: "palavra=numero"
    val counts2 = lines2.flatMap(line => line.replaceAll("[,.!?:;]","").toLowerCase().split(" "))
                 .filter(word => word.length() > 3)
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
    
    val output2 = counts2.sortBy(_._2, false).take(5)
    for ((word, count) <- output2) {
        println(word + "=" + count)
	}

    println("COMMON")
	
    // TODO: comparar resultado e imprimir na ordem ALFABETICA todas as palavras que aparecem MAIS que 100 vezes nos 2 textos
    // imprimir na cada linha: "palavra"
    
    val count_filter_1 = counts1.filter( tuple => tuple._2 > 100).map( item => item._1 )
    val count_filter_2 = counts2.filter( tuple => tuple._2 > 100).map( item => item._1 )
	
	val list_inter = count_filter_1.intersection(count_filter_2).collect.toList
	
	for (word <- list_inter.sorted) {
        println(word)
	}
	
	//println( (System.nanoTime - t1) / 1e9d )
    
    //sc.stop()
  }

}
