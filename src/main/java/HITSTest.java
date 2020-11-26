import Utils.GlobalVariances;
import Utils.JdbcUtil;
import Utils.SQLUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class HITSTest {
    public Map<Integer,String> id2uri;   //
    private Set<Integer> literal = new HashSet<>();//literal的id集合
    int typeID;
    Connection connection_remote;
    Connection connection_local;
    private HashMap<Integer,Integer> entity2id;
    private HashMap<Integer,Integer> id2entity;
    private Hits hits;
    private int nodeCount;

    public static void main(String args[]){
        HITSTest hitsTest = new HITSTest();
//        hitsTest.getNodeCount(2,1);
//        for(int i=308;i<=311;i++){ //21   99   145    176  181  235   248  280   297 299  301   306  307  308and
//
//            hitsTest.id2entity.clear();
//            hitsTest.id2uri.clear();
//            hitsTest.entity2id.clear();
//            hitsTest.nodeCount=-1;
//            hitsTest.typeID=-1;
//
//            hitsTest.readDataBase(2,i);
//            System.out.println("dataset "+i+" end!");
//
//
//        }

        for(int i=1624;i<=9318;i++){ //1623

            hitsTest.id2entity.clear();
            hitsTest.id2uri.clear();
            hitsTest.entity2id.clear();
            hitsTest.nodeCount=-1;
            hitsTest.typeID=-1;

            hitsTest.readDataBase(3,i);
            System.out.println("dataset "+i+" end!");


        }
    }

    public HITSTest(){

        connection_remote = JdbcUtil.getConnection(GlobalVariances.REMOTE);
        connection_local = JdbcUtil.getConnection(GlobalVariances.LOCAL);
        id2uri = new HashMap<>();//id -> uri

        entity2id = new HashMap<>();
        id2entity = new HashMap<>();
        nodeCount = -1;
        typeID=-1;
//        hits = new Hits();

    }

    public void getNodeCount(Integer table_id,Integer dataset_local_id){
        String getTripleCount = String.format("SELECT * FROM others%d WHERE dataset_local_id=%d;",table_id,dataset_local_id);
        try{
            Statement ssst = connection_local.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rrrst = ssst.executeQuery(getTripleCount);

            while(rrrst.next()){
                nodeCount = rrrst.getInt("node_count_for_hits");
            }
        }catch (Exception e){
            e.printStackTrace();

        }

//        return tripleCount;
    }

    public void readDataBase(Integer table_id,Integer dataset_local_id){
        getID2URI(table_id,dataset_local_id);
        getNodeCount(table_id,dataset_local_id);
        hits = new Hits(nodeCount);
//        typeID=-1;
//        hits = new Hits(3);




        String sql = String.format("SELECT * FROM triple%d WHERE dataset_local_id=%d",table_id,dataset_local_id);
        try {
            Statement pst = connection_remote.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rst = pst.executeQuery(sql);

            int idx = 0;
            while (rst.next()) {
//                System.out.println("test");

                int subject = rst.getInt("subject");
                int predicate = rst.getInt("predicate");
                int object = rst.getInt("object");

                if (predicate != typeID && !literal.contains(object)) {
                    int subidx=-1,objidx=-1;
                    if(!entity2id.containsKey(subject)){
                        entity2id.put(subject,idx);
                        subidx=idx++;
                    }else{
                        subidx = entity2id.get(subject);
                    }

                    if(!entity2id.containsKey(object)) {
                        entity2id.put(object,idx);
                        objidx=idx++;
                    }else{
                        objidx = entity2id.get(object);
                    }

                    id2entity.put(subidx,subject);
                    id2entity.put(objidx,object);

                    hits.addEdge(subidx,objidx);


                }


            }
            rst.close();

            List<double[]> result = hits.printResultPage();
            double[] rhub = result.get(0);
//            System.out.println("rhub:");
//            for(double dd:rhub){
//                System.out.println(dd);
//            }
//            System.out.println("===============================================");
            double[] rauthority = result.get(1);

            HashMap<Integer,Double> resHub = new HashMap<>();//entity  - score
            HashMap<Integer,Double> resAuth = new HashMap<>();

            //只存前50个好了
            for(int i=0;i<rhub.length;i++){
                if(id2entity.containsKey(i)){
                    resHub.put(id2entity.get(i),rhub[i]);
                }

            }
            for(int i=0;i<rauthority.length;i++){
                if(id2entity.containsKey(i)){
                    resAuth.put(id2entity.get(i),rauthority[i]);
                }
            }

            LinkedHashMap<Integer,Double> resHub2 = sortHashMap(resHub);
            LinkedHashMap<Integer,Double> resAuth2 = sortHashMap(resAuth);

//            System.out.println("resHub2:");
//            for(Integer tt:resHub2.keySet()){
//                System.out.println(tt+":"+resHub2.get(tt));
//            }
//            System.out.println("resaU2:");
//            for(Integer tt:resAuth2.keySet()){
//                System.out.println(tt+":"+resAuth2.get(tt));
//            }

            /***写入***/
            String hubw = String.format("INSERT INTO hits%d(dataset_local_id,entity,hub)values(?,?,?);",table_id);
            PreparedStatement pstw = connection_local.prepareStatement(hubw);
            int count =0;
            for(Integer entity_id : resHub2.keySet()){
//                System.out.println("entity:"+entity_id);
                pstw.setInt(1,dataset_local_id);
                pstw.setString(2,SQLUtil.getURIForId(table_id,dataset_local_id,entity_id));
                pstw.setString(3,String.format("%.8f",resHub2.get(entity_id)));
                pstw.executeUpdate();
                count++;
                if(count>50)break;
            }

            count=0;
            String authw = String.format("INSERT INTO HITS%d(dataset_local_id,entity,authority)values(?,?,?);",table_id);
            pstw = connection_local.prepareStatement(authw);
            for(Integer entity_id : resAuth2.keySet()){
                pstw.setInt(1,dataset_local_id);
                pstw.setString(2,SQLUtil.getURIForId(table_id,dataset_local_id,entity_id));
                pstw.setString(3,String.format("%.8f",resAuth2.get(entity_id)));
                pstw.executeUpdate();
                count++;
                if(count>50)break;
            }

            pstw.close();

//            connection_local.close();


        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void getID2URI(int tableid,int dataset_local_id){

        id2uri.clear();
        literal.clear();



        String selectLabel = String.format("select * from uri_label_id%d where dataset_local_id = %d",tableid,dataset_local_id);

        try {
            PreparedStatement selectStatement = connection_remote.prepareStatement(selectLabel);
            ResultSet resultSet = selectStatement.executeQuery();


            while (resultSet.next()){
                int id = resultSet.getInt("id");
                String label = resultSet.getString("label");
//                System.out.println(label);
                String uri = resultSet.getString("uri");
//                System.out.println(uri);
                boolean litr = resultSet.getBoolean("is_literal");

                if("type".equals(label)){
                    typeID = id;
                }

//                if("subClass".equals(label)){
//                    subClassID=id;
//                }

                id2uri.put(id,uri);
                if (litr){ //是literal
                    literal.add(id);
                }
            }
            /**labelID建完*/
            resultSet.close();
            selectStatement.close();

//            connection_remote.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static LinkedHashMap<Integer,Double> sortHashMap(HashMap<Integer,Double> map){
        //從HashMap中恢復entry集合，得到全部的鍵值對集合
        Set<Map.Entry<Integer,Double>> entey = map.entrySet();
        //將Set集合轉為List集合，為了實用工具類的排序方法
        List<Map.Entry<Integer,Double>> list = new ArrayList<Map.Entry<Integer,Double>>(entey);
        //使用Collections工具類對list進行排序
        Collections.sort(list, new Comparator<Map.Entry<Integer, Double>>() {
            @Override
            public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {
                //按照age倒敘排列
                if(o1.getValue()-o2.getValue()>0) return -1;
                else if(o1.getValue()-o2.getValue()<0) return 0;
                else return 0;
            }
        });
        //創建一個HashMap的子類LinkedHashMap集合
        LinkedHashMap<Integer,Double> linkedHashMap = new LinkedHashMap<Integer,Double>();
        //將list中的數據存入LinkedHashMap中
        for(Map.Entry<Integer,Double> entry:list){
            linkedHashMap.put(entry.getKey(),entry.getValue());
        }
        return linkedHashMap;
    }




}
