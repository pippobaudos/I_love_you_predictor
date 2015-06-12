package org.hackatown.lovePredictor;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import org.hackatown.lovePredictor.dao.*;
import org.hackatown.lovePredictor.dao.Character;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by niccolo on 6/11/15.
 */
public class CsvLoader {

    static JavaPairRDD<String, Line> loadLinesRdd(String filePath, JavaSparkContext sc, SQLContext sqlContext) {
        return sc.textFile(filePath).mapToPair(new PairFunction<String, String, Line>() {
            @Override
            public Tuple2<String, Line> call(String line) throws Exception {
                String[] p = line.split("@");
                Line l = new Line();
                l.setLineId(p[0]);
                l.setUserId(p[1]);
                l.setMovieId(p[2]);
                l.setName(p[3]);
                l.setConversation(p.length>=5?p[4]:"");
                return new Tuple2<String, Line>(p[0], l);
            }
        });
    }


    static JavaRDD<Character> loadCharactersRdd(String filePath, JavaSparkContext sc, SQLContext sqlContext) {
        JavaRDD<org.hackatown.lovePredictor.dao.Character> character = sc.textFile(filePath).map(
                new Function<String, Character>() {
                    public Character call(String line) throws Exception {
                        String[] p = line.split("@");
                        Character c = new Character();
                        c.setUserId(p[0]);
                        c.setName(p[1]);
                        c.setMovieId(p[2]);
                        c.setTitle(p[3]);
                        c.setGender(p[4]);
                        //try {
                        //    c.setAge(Integer.parseInt(p[5]));
                        //} catch (Exception e) {
                        //}
                        return c;
                    }
                });

        return character;
    }

    static JavaPairRDD<String, Conversation> loadConversationsExplodedRdd(String filePath, JavaSparkContext sc, SQLContext sqlContext) {
        return sc.textFile(filePath).flatMapToPair(new PairFlatMapFunction<String, String, Conversation>() {
            @Override
            public Iterable<Tuple2<String, Conversation>> call(String line) throws Exception {
                List<Tuple2<String, Conversation>> listOut = new ArrayList<>();
                // Build List of Conversations
                String[] p = line.split("@");
                String ids = p[3].replace("[","").replace("]", "").replace("'","").replace(" ","");
                String[] conversationIds = ids.split(",");
                for (String conversationId : conversationIds) {
                    Conversation c = new Conversation();
                    c.setUserId1(p[0]);
                    c.setUserId2(p[1]);
                    c.setMovieId(p[2]);
                    c.setLine(conversationId);

                    if (conversationId.startsWith("L")) {
                        c.setIdLine(Integer.parseInt(conversationId.substring(1)));
                    }

                    listOut.add(new Tuple2<String, Conversation>(conversationId, c));
                }
                return listOut;
            }
        });
    }
}
