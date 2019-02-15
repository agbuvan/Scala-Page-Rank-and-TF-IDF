import org.apache.spark.{SparkConf, SparkContext}

object PageRank {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("pagerank"));
    val filepath = args(0);
    val iterations = args(1).toInt;
    val outpath = args(2);
    val data_wheader = sc.textFile(filepath);
    val header = data_wheader.first()
    val data = data_wheader.filter(row => row != header)

    val pairs = data.map{ line =>
      val row = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
      (row(1), row(3))
    }
    val nodes_compact = pairs.distinct().groupByKey().cache()
    val nodes = nodes_compact.map(x => (x._1, x._2.toList)).cache()
    var ranks = nodes.mapValues(v => 10.0)

    for (i <-1 to iterations){
      val pageranks = nodes.join(ranks)
        .values
        .flatMap{ case(airports, rank) =>
          val size = airports.size
          airports.map(airport => (airport, rank/size))
        }
      ranks = pageranks.reduceByKey(_+_).mapValues(v => 0.15 + 0.85*v).sortBy(x => -x._2)
    }
    ranks.saveAsTextFile(outpath)
  }
}
