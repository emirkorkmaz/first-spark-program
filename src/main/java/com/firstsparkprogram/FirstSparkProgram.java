package com.firstsparkprogram;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FirstSparkProgram {

    public static void main(String[] args) {

        //This is the first spark program
        //It will be executed in local without having necessity of a cluster
        //And of course it will be a word count example :)

        //We'll be using Spark RDD for this first spark program

        //01. Get rid of unnecessary logging, we dont need at this time
        Logger.getLogger("org").setLevel(Level.ERROR);

        //02. Create Spark Conf and Context. A spark program using RDDs needs these
        //by doing setMaster("local[*]") we're telling that our driver program will work on local resources and use
        //all available CPU cores
        SparkConf sparkConf = new SparkConf().setAppName("FirstSparkProgram").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //03. Ready to start, lets read a file
        JavaRDD<String> data = jsc.textFile("src/main/resources/input/loremipsum.text");

        //04. Read each line and get each word in each line
        //In single line, there are many words, relation is 1 - n, so we'll use flatMap
        //In sample text, there are many dots and commas, necessary to remove them all.
        JavaRDD<String> words = data.flatMap(FirstSparkProgram::call);

        //05. Starting marking occurence of each word. When we detect a word, we will mark it as 1
        //Here we are better to use a key value approach (word, 1) that is PairRDD and Tuples
        JavaPairRDD<String, Integer> wordOccurenceCounted = words.mapToPair(word -> new Tuple2<>(word, 1));

        //06. At this step we have a key - value pair. For counting occurence of each key, we can use reduceByKey
        //Output will be a Tuple and so PairRDD again
        //By each key, reduceByKey will process occurence of each word
        JavaPairRDD<String, Integer> wordCount = wordOccurenceCounted.reduceByKey((occW1, occW2) -> occW1 + occW2);

        //you can print our the result or directly write to text file
        wordCount.saveAsTextFile("src/main/resources/output/loremipsumcount.text");
    }

    private static Iterator<String> call(String line) {
        String removingDotAndComma = line.replaceAll("\\.", "").
                replaceAll("\\,", "");

        return Arrays.asList(removingDotAndComma.split(" ")).iterator();
    }
}
