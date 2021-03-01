import Utils.FileModel;
import Utils.GlobalVariances;
import Utils.JdbcUtil;
import com.alibaba.fastjson.JSON;
import net.sf.json.JSONObject;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class ToolGuy {
    Connection connection_remote;
    Connection connection_local;

    Set<Integer> literal;
    int typeID;

    List<Default> database_info;

    Map<Integer,Integer> datasetID2maxDegreeNodeID;



    public ToolGuy() {
        getDefaultList();
        this.connection_remote = JdbcUtil.getConnection(GlobalVariances.REMOTE);
        this.connection_local = JdbcUtil.getConnection(GlobalVariances.LOCAL);
        datasetID2maxDegreeNodeID = new HashMap<>();
        literal = new HashSet<>();
        typeID = -1;

        database_info = new ArrayList<>(); database_info.clear();
//        getDefaultList();
    }

    public void getMaxOutdegreeNode(int table_id,int dataset_local_id){
        datasetID2maxDegreeNodeID.clear();
        String sql = String.format("SELECT * FROM triple%d WHERE dataset_local_id=%d;",table_id,dataset_local_id);

        try{
            Statement stmt = connection_remote.createStatement();
            ResultSet rst = stmt.executeQuery(sql);

            while(rst.next()){
                int sub = rst.getInt("subject");
                int pre = rst.getInt("predicate");
                int obj = rst.getInt("object");

                if(datasetID2maxDegreeNodeID.containsKey(sub)){
                    datasetID2maxDegreeNodeID.put(sub,datasetID2maxDegreeNodeID.get(sub)+1);
                }else{
                    datasetID2maxDegreeNodeID.put(sub,1);
                }
            }
            List<Map.Entry<Integer, Integer>> infoIds = new ArrayList<Map.Entry<Integer,Integer>>(datasetID2maxDegreeNodeID.entrySet());
            Collections.sort(infoIds, new Comparator<Map.Entry<Integer, Integer>>() {
                public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                    return (o2.getValue() - o1.getValue());
                    //return (o1.getKey()).toString().compareTo(o2.getKey());
                }
            });


            String insert = String.format("INSERT INTO datasetid2maxdegreenodeid%d(dataset_local_id,maxOutdegreeNodeID) values (?,?);",table_id);
            PreparedStatement pstmt=connection_local.prepareStatement(insert);
            pstmt.setInt(1,dataset_local_id);
            pstmt.setInt(2,infoIds.get(0).getKey());
            pstmt.executeUpdate();

            pstmt.close();
            rst.close();
            stmt.close();



        }catch(Exception e){
            e.printStackTrace();
        }

    }



    public void getNamespace(int dataset_local_id){
        int table_id = 2;
        if(dataset_local_id>311){
            table_id=3;
            dataset_local_id-=311;
        }

//        getTypeID(table_id,dataset_local_id);

        HashMap<String,String> vocab2prefix = new HashMap<>();




        String selectLabel = String.format("select * from uri_label_id%d where dataset_local_id = %d AND uri LIKE '%s' AND is_literal=0",table_id,dataset_local_id,"http%");

        try {
            FileModel.CreateFolder("D:\\Index\\Namespace2\\"+dataset_local_id);
            Directory dir = FSDirectory.open(Paths.get("D:\\Index\\Namespace2\\"+dataset_local_id));//会变的，一个dataset一个文件夹，因为是一个group一个document！！
            IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            config.setMaxBufferedDocs(100);
            IndexWriter indexWriter = new IndexWriter(dir, config);


            PreparedStatement selectStatement = connection_remote.prepareStatement(selectLabel);
            ResultSet resultSet = selectStatement.executeQuery();




            int idx = 0;//namespace标号
            String lastURI = "";

            if(resultSet.next()){
                String uri = resultSet.getString("uri");
//                System.out.println("uri:"+uri);
                String label = resultSet.getString("label")==null?"":resultSet.getString("label");
                lastURI = getUriPre(uri,label);
            }


            while (resultSet.next()){
                int id = resultSet.getInt("id");
                String label = resultSet.getString("label")==null?"":resultSet.getString("label");
                String uri = resultSet.getString("uri");
                int is_literal = resultSet.getInt("is_literal");
//                System.out.println("uri:"+uri);

                //先去除label，获取前半段
                String uriPre = getUriPre(uri,label);

                if(uriPre.equals(lastURI)) continue;
                else{
                    if(!vocab2prefix.containsKey(lastURI)) {
                        String defaultPre = getDefaultPrefix(lastURI);

                        //获取缩写
                        if("".equals(defaultPre)){
                            vocab2prefix.put(lastURI,"ns"+idx);
                            idx++;

                        }else{
                            vocab2prefix.put(lastURI,defaultPre);
                        }




                    }
                    lastURI = uriPre;
                }
            }

            //处理最后一个
            if(!vocab2prefix.containsKey(lastURI)) {
                //获取缩写
                String defaultPre = getDefaultPrefix(lastURI);

                //获取缩写
                if("".equals(defaultPre)){
                    vocab2prefix.put(lastURI,"ns"+idx);
                    idx++;

                }else{
                    vocab2prefix.put(lastURI,defaultPre);
                }


            }

//            System.out.println("prefix\tvocabulary");
            /**建索引**/
            for(String vocab : vocab2prefix.keySet()){
//                System.out.println(vocab2prefix.get(vocab)+"\t"+vocab);


                Document doc = new Document();
                doc.add(new StringField("prefix",vocab2prefix.get(vocab), Field.Store.YES));
                doc.add(new StringField("vocabulary",vocab, Field.Store.YES));

                indexWriter.addDocument(doc);
            }
            /***/


            indexWriter.commit();

            indexWriter.close();
            dir.close();

        }catch (Exception e){
            e.printStackTrace();
        }




    }

    private String getUriPre(String uri,String label){
        String one = uri.replace(label,"");

        return one;

    }

    private void getDefaultList(){
        try
        {
            File file = new File("D:\\workplace\\calForDatasetdashboard\\src\\main\\java\\defaultRDF.json");
            Scanner sc = new Scanner(file);
            while(sc.hasNextLine())
            {
                String line = sc.nextLine();
                database_info = JSON.parseArray(line, Default.class);
            }
            sc.close();
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
    }

    private String getDefaultPrefix(String str){
        int pos = str.indexOf("#");
        String result = "";
        //TODO   看默认的里面有无
         for(Default de : database_info){
             if(str.contains(de.getValue())){
                 result = de.getName();
                 break;
             }
         }

        return result;
    }

    //TODO 需要修改
    private String getBreakStrPrefix(String str){
        str = str.replace("///","/");
//        System.out.println("str:"+str);

        String result = "";
        int beforePos = str.indexOf("//");
        if(beforePos==-1) return str;
        else{
            int afterPos = str.lastIndexOf("#");
            if(afterPos!=-1){
                //循环获取/
                String prefix = getShortForm(str.substring(beforePos+2,afterPos));
//                String label = str.substring(afterPos+1);
                result=prefix;
            }else{
                afterPos = str.lastIndexOf("/");
                String prefix = getShortForm(str.substring(beforePos+2,afterPos));

                result=prefix;
            }
        }

        return result;
    }

    private String getShortForm(String str){
//        System.out.println("getShortForm str:"+str);
        int pos = 0;

        String result = "";

        while(pos!=-1){
            result += str.substring(pos==0?pos:pos+1,pos==0?pos+1:pos+2);

            pos=str.indexOf("/",pos+1);
        }

        return result;
    }

    public void getNodeCount(int table_id, int dataset_local_id){
        Set<Integer> nodes = new HashSet<>();

        getliteralAndTypeID(table_id,dataset_local_id);

        ResultSet rst = getTriples(table_id,dataset_local_id);
        try{
            while(rst.next()){
                int subject = rst.getInt("subject");
                int predicate = rst.getInt("predicate");
                int object = rst.getInt("object");
                if (predicate != typeID && !literal.contains(object)) {
                    nodes.add(subject);
                    nodes.add(object);


                }


            }

            String in = String.format("INSERT INTO others%d(dataset_local_id,node_count_for_hits)VALUES(?,?);",table_id);
            PreparedStatement inn = connection_local.prepareStatement(in);
            inn.setInt(1,dataset_local_id);
            inn.setInt(2,nodes.size());
            inn.executeUpdate();

            inn.close();rst.close();
        }catch (Exception e){
            e.printStackTrace();
        }



    }

    public void getliteralAndTypeID(int table_id,int dataset_local_id){

//        id2uri.clear();
        literal.clear();



        String selectLabel = String.format("select * from uri_label_id%d where dataset_local_id = %d",table_id,dataset_local_id);

        try {
            PreparedStatement selectStatement = connection_remote.prepareStatement(selectLabel);
            ResultSet resultSet = selectStatement.executeQuery();


            while (resultSet.next()){
                int id = resultSet.getInt("id");
                String label = resultSet.getString("label");

                String uri = resultSet.getString("uri");

                boolean litr = resultSet.getBoolean("is_literal");

                if("type".equals(label)){
                    typeID = id;
                }

                if (litr){ //是literal
                    literal.add(id);
                }
            }
            /**labelID建完*/
            resultSet.close();
            selectStatement.close();

//            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void getTypeID(int table_id,int dataset_local_id){




        String selectLabel = String.format("select * from uri_label_id%d where dataset_local_id = %d",table_id,dataset_local_id);

        try {
            PreparedStatement selectStatement = connection_remote.prepareStatement(selectLabel);
            ResultSet resultSet = selectStatement.executeQuery();


            while (resultSet.next()){
                int id = resultSet.getInt("id");
                String label = resultSet.getString("label");



                if("type".equals(label)){
                    typeID = id;
                    return;
                }


            }
            /**labelID建完*/
            resultSet.close();
            selectStatement.close();

//            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private ResultSet getTriples(int table_id,int dataset_local_id){
        String sql = String.format("SELECT * FROM triple%d Where dataset_local_id=%d",table_id,dataset_local_id);
        ResultSet rst = null;
        try{
            PreparedStatement pst = connection_remote.prepareStatement(sql);
            rst = pst.executeQuery();
        }catch (Exception e){
            e.printStackTrace();
        }

        return rst;
    }

    private void getPrefixAndVocabulary(int table_id,int dataset_local_id){

        String sel = String.format("SELECT * FROM namespace%d WHERE dataset_local_id=%d;");

    }
}
