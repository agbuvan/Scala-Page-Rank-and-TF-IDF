import org.apache.spark.{SparkConf, SparkContext}

object MoviePlot {
  // tf-idf weight is a statistical measure used to evaluate how important a word is to a document in a corpus
  // term frequency is normalized to prevent bias towards larger documents
  // inverse document frequency (idf) is obtained by dividing total no. of documents by no. of documents containing term t

    def main(args: Array[String]): Unit = {
      val sc = new SparkContext(new SparkConf().setAppName("Spark tfidf"));
      val filepath = args(0);
      val metadata = args(1);
      val outpath = args(2);
      val userinput = args(3);
      val movieplots = sc.textFile(filepath).cache();
      val stopwords = Set("'tis","'twas","a","able","about","across","after","ain't","all","almost",
        "also","am","among","an","and","any","are","aren't","as","at","be","because","been","but","by","can","can't","cannot","could","could've","couldn't","dear","did",
        "didn't","do","does","doesn't","don't","either","else","ever","every","for","from","get","got","had","has","hasn't","have","he","he'd","he'll","he's","her",
        "hers","him","his","how","how'd","how'll","how's","however","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","just","least","let","like",
        "likely","may","me","might","might've","mightn't","most","must","must've","mustn't","my","neither","no","nor","not","of","off","often","on","only","or","other",
        "our","own","rather","said","say","says","shan't","she","she'd","she'll","she's","should","should've","shouldn't","since","so","some","than","that","that'll",
        "that's","the","their","them","then","there","there's","these","they","they'd","they'll","they're","they've","this","tis","to","too","twas","us","wants","was","wasn't",
        "we","we'd","we'll","we're","were","weren't","what","what'd","what's","when","when","when'd","when'll","when's","where","where'd","where'll","where's","which",
        "while","who","who'd","who'll","who's","whom","why","why'd","why'll","why's","will","with","won't","would","would've","wouldn't","yet","you","you'd",
        "you'll","you're","you've","your");

      val pairs = movieplots.map(x => (x.split("\t")(0), x.split("\t")(1).
        replaceAll("""[\p{Punct}]""", "").toLowerCase().split("\\s+").
        filter(!stopwords.contains(_)).toSeq)).cache()

      val nums= pairs.count
      val tdoc=pairs.flatMapValues(x=>x).countByValue.toSeq
      val temp=tdoc.map(x=>(x._1._2,1))
      val rdd= sc.parallelize(temp)
      val reduced =rdd.reduceByKey(_+_).collect()
      val tffin=tdoc.filter{case(x, y) => x._2==(userinput)}
      val idffin=nums.toDouble/reduced.filter{case(x, y) => x==(userinput)}(0)._2
//      val pro= tffin.map(x=>(x._1._1,x._2*idffin))
      val topmovies=tffin.map(x=>(x._1._1,x._2*idffin)).sortBy(-_._2).take(10);
      val moviemeta = sc.textFile(metadata).map(x=>(x.split("\t")(0),x.split("\t")(2))).cache()
      val rdd_topmovies = sc.parallelize(topmovies)
      val output = moviemeta.join(rdd_topmovies).sortBy(-_._2._2)
      output.saveAsTextFile(outpath)

    }

}
