package Utils;

import net.sf.json.JSONObject;
//import org.apache.lucene.analysis.Analyzer;
//import org.apache.lucene.analysis.TokenStream;
//import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.*;

public class Tools
{
    public static Set<Integer> SetUnion(Set<Integer> s1, Set<Integer> s2)
    {
        Set<Integer> ret = new HashSet<>(); ret.clear();
        ret.addAll(s1); ret.addAll(s2);
        return ret;
    }

    public static void SetUnion_Self(Set<Integer> s1, Set<Integer> s2)
    {
        s1.addAll(s2);
    }

    public static Set<Integer> SetIntersection(Set<Integer> s1, Set<Integer> s2)
    {
        Set<Integer> ret = new HashSet<>(); ret.clear();
        for(Integer v : s1) if(s2.contains(v)) ret.add(v);
        return ret;
    }

    public static void SetIntersection_Self(Set<Integer> s1, Set<Integer> s2)
    {
        Set<Integer> ret = new HashSet<>(); ret.clear();
        for(Integer v : s1) if(s2.contains(v)) ret.add(v);
        s1 = ret;
    }

    public static String getCurrentTime()
    {
        SimpleDateFormat sdf = new SimpleDateFormat();// 格式化时间
        sdf.applyPattern("yyyy-MM-dd HH:mm:ss");// a为am/pm的标记
        Date date = new Date();// 获取当前时间
        return sdf.format(date);
    }

    public static JSONObject readJson(String filename)
    {
        File file = new File(filename);
        if(!file.exists()) return new JSONObject();
        String text = "";
        try
        {
            Scanner sc = new Scanner(file);
            while(sc.hasNextLine())
            {
                String line = sc.nextLine();
                line = line.replace("\n", "").replace(" ", "").replace("\t", "");
                text += line;
            }
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        System.out.println( filename + " Load Success!" );
        return JSONObject.fromObject(text);
    }

    public static String insertInto(String temp, List<String> list)
    {
        if(list.size() <= 0) return "";
        String result = list.get(0).toString();
        for(Integer i = 1; i < list.size(); ++ i) result += temp + list.get(i).toString();
        return result;
    }


}
