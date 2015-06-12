package org.hackatown.lovePredictor;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import org.hackatown.lovePredictor.dao.Character;
import org.hackatown.lovePredictor.dao.Conversation;
import org.hackatown.lovePredictor.dao.Line;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Process most of the CSV included in movie-dialogs  and build a dataset usable to build a classifier usable to predict if in the next conversation
 *  there an "I love you!!"
 *
 */
public class DataSetBuilder {


    static final int SIZE_CONVERSATIONS = 5;



    public void loader(SparkConf scConf) {


    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expected one input argument in input containing the path of the movie-dialogs dataset, e.g: /home/niccolo/work/hackatown-games/movie-dialogs/") ;
        }

        String path0 = args[0]; //"/home/niccolo/work/hackatown-games/movie-dialogs/";
        String PATH_OUTPUT_FILE = path0 + "pre_love_built_dataset";

        File f = new File(PATH_OUTPUT_FILE);
        if (f.exists()) {
            FileUtils.deleteDirectory(f);
        }

        SparkConf scConf = new SparkConf().setAppName("BuildLovePredictorDataset").setMaster("local[2]").set("spark.executor.memory", "1g");
        JavaSparkContext sc = new JavaSparkContext(scConf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaPairRDD<String, Line> linesPairRdd = CsvLoader.loadLinesRdd(path0 + "movie_lines.txt", sc, sqlContext);
        JavaPairRDD<String, Conversation> conversations0 = CsvLoader.loadConversationsExplodedRdd(path0 + "movie_conversations.txt", sc, sqlContext);
        JavaPairRDD<Integer, Conversation> conversations1 = addTextContentToConversation(conversations0, linesPairRdd);
        JavaRDD<Conversation> conversations2 = groupAllTheLinesOnEachConversation(conversations1);
        JavaRDD<Conversation> conversationsWithUser1And2 = addGendersToUsersInConversation(path0 + "movie_characters_metadata.txt", sc, sqlContext, conversations2);
        JavaRDD<Conversation> conversationsFemaleMale = filterOnlyFemaleMaleAncCheckILoveYou(conversationsWithUser1And2);
        JavaRDD<Conversation> conversationsSelectedForEachCouple = selectDataForEachCoupleOfUsers(conversationsFemaleMale);
        conversationsSelectedForEachCouple.cache();
        Map<Boolean, Integer> mapCounts = countNegativeAndPositiveCases(conversationsSelectedForEachCouple);

        final int finalNumLove = mapCounts.get(true);
        final int finalNumNotLove = mapCounts.get(false);
        JavaRDD<Conversation> downsampledNegativeCases = downsampleNegativeCases(conversationsSelectedForEachCouple, finalNumLove, finalNumNotLove);

        saveCsvFile(PATH_OUTPUT_FILE, downsampledNegativeCases);

    }

    private static void saveCsvFile(String PATH_OUTPUT_FILE, JavaRDD<Conversation> downsampledNegativeCases) {
        downsampledNegativeCases.map(new Function<Conversation, String>() {
            boolean isFirstLine = true;

            @Override
            public String call(Conversation conv) throws Exception {
                StringBuilder sb = new StringBuilder();
                if (isFirstLine) {
                    sb.append("target,movie_users_id,first_I_love_you_conversation,conversationsIds_before_the_first_I_love_you_or_just_randomly_choosen,text_content\n");
                    isFirstLine = false;
                }
                sb.append((conv.getLove() ? "pre_love" : "neutral") + "," + conv.getKeyMoviedUsers() + "," + conv.getFirstILoveYouConversation().replaceAll(",", " ") + "," + conv.getLine() + "," + conv.getContent().replaceAll(",", " "));
                return sb.toString();
            }
        }).coalesce(1).saveAsTextFile(PATH_OUTPUT_FILE);
    }

    private static JavaRDD<Conversation> downsampleNegativeCases(JavaRDD<Conversation> conversationsSelectedForEachCouple, final int finalNumLove, final int finalNumNotLove) {
        return conversationsSelectedForEachCouple.filter(new Function<Conversation, Boolean>() {
                @Override
                public Boolean call(Conversation c) throws Exception {
                    if (c.getLove())
                        return true;
                    else
                        return Math.random() < finalNumLove * 1.0 / finalNumNotLove;
                }
            });
    }

    private static Map<Boolean, Integer> countNegativeAndPositiveCases(JavaRDD<Conversation> conversationsSelectedForEachCouple) {
        JavaPairRDD<Boolean,Integer> pairs = conversationsSelectedForEachCouple.mapToPair(new PairFunction<Conversation, Boolean, Integer>() {
            public Tuple2<Boolean, Integer> call(Conversation c) {
                return new Tuple2<Boolean, Integer>(c.getLove(), 1);
            }
        });
        JavaPairRDD<Boolean, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });
        return counts.collect().stream().collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
    }

    private static JavaRDD<Conversation> selectDataForEachCoupleOfUsers(JavaRDD<Conversation> conversationsFemaleMale) {
        return conversationsFemaleMale.mapToPair(new PairFunction<Conversation, String, Conversation>() {
                @Override
                public Tuple2<String, Conversation> call(Conversation c) throws Exception {
                    return new Tuple2<String, Conversation>(c.getKeyMoviedUsers(), c);
                }
            }).groupByKey().flatMapValues(new Function<Iterable<Conversation>, Iterable<Conversation>>() {
                @Override
                public Iterable<Conversation> call(Iterable<Conversation> listIn) throws Exception {

                    // Apply Secondary Sort, perhaps there is a better way to do this in SPARK
                    List<Conversation> allConversationsUser1_2 = new ArrayList<>();
                    for (Conversation c : listIn) {
                        allConversationsUser1_2.add(c);
                    }
                    Collections.sort(allConversationsUser1_2, new Comparator<Conversation>() {
                        @Override
                        public int compare(Conversation o1, Conversation o2) {
                            return (int) Math.signum(o1.getIdLine() - o2.getIdLine());
                        }
                    });

                    OptionalInt iFirstLove = IntStream.range(0, allConversationsUser1_2.size())
                            .filter(i -> allConversationsUser1_2.get(i).getLove())
                            .findFirst();

                    // Don't mark as loving in evolution
                    boolean isInLove = iFirstLove.isPresent();
                    if (isInLove && iFirstLove.getAsInt() == 0) {
                        return Arrays.asList();
                    }

                    int i0 = isInLove ? iFirstLove.getAsInt()-1 : (int)(Math.random()*allConversationsUser1_2.size());
                    Conversation convToAggregateOn = allConversationsUser1_2.get(i0);
                    if (isInLove) {
                        Conversation firstLoveConversation = allConversationsUser1_2.get(iFirstLove.getAsInt());
                        convToAggregateOn.setFirstILoveYouConversation("(" + firstLoveConversation.getIdLine() + ") " + firstLoveConversation.getContent());
                    }

                    convToAggregateOn.setLove(isInLove);
                    for (int iAggr = 1; iAggr <= SIZE_CONVERSATIONS && i0 - iAggr >= 0; iAggr++) {
                        convToAggregateOn.addContentAndLine(allConversationsUser1_2.get(i0 - iAggr), " | ");
                    }
                    return Arrays.asList(convToAggregateOn);
                }
            }).values();
    }

    private static JavaRDD<Conversation> filterOnlyFemaleMaleAncCheckILoveYou(JavaRDD<Conversation> conversationsWithUser1And2) {
        return conversationsWithUser1And2.filter(new Function<Conversation, Boolean>() {
            @Override
            public Boolean call(Conversation conversation) throws Exception {
                return "fm".equals(conversation.getGenders());
            }
        }).map(new Function<Conversation, Conversation>() {
            @Override
            public Conversation call(Conversation c) throws Exception {
                c.setLove(c.getContent().toLowerCase().contains("i love you"));
                return c;
            }
        });
    }

    private static JavaRDD<Conversation> addGendersToUsersInConversation(String pathFile, JavaSparkContext sc, SQLContext sqlContext, JavaRDD<Conversation> conversations2) {
        JavaRDD<Character> schemaCharacter = CsvLoader.loadCharactersRdd(pathFile, sc, sqlContext);
        JavaPairRDD<String,String> characterMapped = schemaCharacter.mapToPair(new PairFunction<Character, String, String>() {
            @Override
            public Tuple2<String, String> call(Character ch) throws Exception {return new Tuple2(ch.getUserId(), ch.getGender());
            }
        });


        JavaPairRDD<String, Iterable<Conversation>> conversationOnUser1 = conversations2.groupBy(new Function<Conversation, String>() {
            @Override
            public String call(Conversation conversation) throws Exception {
                return conversation.getUserId1();
            }
        });

        JavaRDD<Conversation> conversationsWithUser1 = conversationOnUser1.join(characterMapped).flatMap(new FlatMapFunction<Tuple2<String, Tuple2<Iterable<Conversation>, String>>, Conversation>() {
            @Override
            public Iterable<Conversation> call(Tuple2<String, Tuple2<Iterable<Conversation>, String>> stringTuple2Tuple2) throws Exception {
                List<Conversation> listOut = new ArrayList<>();
                Tuple2<Iterable<Conversation>,String> it = stringTuple2Tuple2._2();
                for( Conversation c : it._1()) {
                    c.setGender1(it._2());
                    listOut.add(c);
                }
                return listOut;
            }
        });

        JavaPairRDD<String, Iterable<Conversation>> conversationOnUser2 = conversationsWithUser1.groupBy(new Function<Conversation, String>() {
            @Override
            public String call(Conversation conversation) throws Exception {
                return conversation.getUserId2();
            }
        });

        return conversationOnUser2.join(characterMapped).flatMap(new FlatMapFunction<Tuple2<String, Tuple2<Iterable<Conversation>, String>>, Conversation>() {
            @Override
            public Iterable<Conversation> call(Tuple2<String, Tuple2<Iterable<Conversation>, String>> stringTuple2Tuple2) throws Exception {
                List<Conversation> listOut = new ArrayList<>();
                Tuple2<Iterable<Conversation>,String> it = stringTuple2Tuple2._2();
                for( Conversation c : it._1()) {
                    c.setGender2(it._2());
                    listOut.add(c);
                }
                return listOut;
            }
        });
    }

    private static JavaRDD<Conversation> groupAllTheLinesOnEachConversation(JavaPairRDD<Integer, Conversation> conversations1) {
        return conversations1.reduceByKey(new Function2<Conversation, Conversation, Conversation>() {
                @Override
                public Conversation call(Conversation c1, Conversation c2) throws Exception {
                    c1.addContentAndLine(c2, ",");
                    return c1;
                }
            }).values();
    }

    private static JavaPairRDD<Integer, Conversation> addTextContentToConversation(JavaPairRDD<String, Conversation> conversations0, JavaPairRDD<String, Line> linesPairRdd) {
        return conversations0.join(linesPairRdd).mapToPair(new PairFunction<Tuple2<String, Tuple2<Conversation, Line>>, Integer, Conversation>() {
                @Override
                public Tuple2<Integer, Conversation> call(Tuple2<String, Tuple2<Conversation, Line>> stringTuple2Tuple2) throws Exception {
                    Conversation c = stringTuple2Tuple2._2()._1();
                    //c.setContent(" (" + c.getContent() + ")-" + stringTuple2Tuple2._2()._2().getConversation());
                    c.setContent(stringTuple2Tuple2._2()._2().getConversation());
                    return new Tuple2<>(c.getIdLine(), c);
                }
            });
    }

}