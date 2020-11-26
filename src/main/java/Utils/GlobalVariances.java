package Utils;


//import org.apache.lucene.analysis.Analyzer;
//import org.apache.lucene.analysis.CharArraySet;
//import org.apache.lucene.analysis.en.EnglishAnalyzer;
//import org.apache.lucene.index.IndexOptions;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class GlobalVariances {
    public static int MAX_CLASS_PROPERTY = 50;
    public static int MIN_CLASS_PROPERTY = 5;
    public static List<Boolean> booleanList = Arrays.asList(false, true);
//    public static List<IndexOptions> indexOptionsList = Arrays.asList(IndexOptions.DOCS, IndexOptions.DOCS_AND_FREQS, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, IndexOptions.NONE);

    public static String storeDir = "db_index";
    public static Integer commit_limit = 10;

    public static String indexDir = "db_index";


    public static String stopWordsPath = "Conf/stopwords.txt";
    public static Set<String> stopWords = null;

    public static Integer maxTripleForGraph2Text = 2000;

    public static final long TIMEOUT = 10000;

    /**for database**/
    public static int LOCAL = 0;
    public static int REMOTE = 1;

    public static Set<String> getStopWords()
    {
        if(null != stopWords) return stopWords;
        stopWords = new HashSet<>(); stopWords.clear();
        try
        {
            Scanner sc = new Scanner(new File( stopWordsPath ));
            while(sc.hasNextLine())
            {
                String str = sc.nextLine();
                stopWords.add(str.replace("\n", "").replace("\r", ""));
            }
            sc.close();
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        return stopWords;
    }


//    public static Analyzer globalAnalyzer(){
//        return new EnglishAnalyzer(new CharArraySet(getStopWords(),true));
//    }
}
