import Utils.GlobalVariances;
import Utils.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

public class ToolGuy {
    Connection connection_remote;
    Connection connection_local;

    Set<Integer> literal;
    int typeID;



    public ToolGuy() {
        this.connection_remote = JdbcUtil.getConnection(GlobalVariances.REMOTE);
        this.connection_local = JdbcUtil.getConnection(GlobalVariances.LOCAL);
        literal = new HashSet<>();
        typeID = -1;
    }

    public void getNamespace(int table_id,int dataset_local_id){
        String selectLabel = String.format("select * from uri_label_id%d where dataset_local_id = %d",table_id,dataset_local_id);

        try {
            PreparedStatement selectStatement = connection_remote.prepareStatement(selectLabel);
            ResultSet resultSet = selectStatement.executeQuery();

            String writeIn = String.format("INSERT INTO namespace%d(id,dataset_local_id,prefixAndlabel)values(?,?,?)",table_id);
            PreparedStatement writePst = connection_local.prepareStatement(writeIn);


            while (resultSet.next()){
                int id = resultSet.getInt("id");
                String label = resultSet.getString("label");
                String uri = resultSet.getString("uri");

                //TODO  在固定的json里有无找到
                String res = getDefaultPrefix(uri);
                if("".equals(res)){
                    res = getBreakStrPrefix(uri);
                }

                writePst.setInt(1,id);
                writePst.setInt(2,dataset_local_id);
                writePst.setString(3,res);
                writePst.executeUpdate();
            }

        }catch (Exception e){
            e.printStackTrace();
        }




    }

    private String getDefaultPrefix(String str){
        int pos = str.indexOf("#");
        String result = "";
        //TODO   看默认的里面有无
        return result;
    }

    private String getBreakStrPrefix(String str){
        String result = "";
        int beforePos = str.indexOf("//");
        if(beforePos==-1) return str;
        else{
            int afterPos = str.lastIndexOf("#");
            if(afterPos!=-1){
                //循环获取/
                String prefix = getShortForm(str.substring(beforePos+2,afterPos));
                String label = str.substring(afterPos+1);
                result=prefix+":"+label;
            }else{
                afterPos = str.lastIndexOf("/");
                String prefix = getShortForm(str.substring(beforePos+2,afterPos));
                String label = str.substring(afterPos+1);
                result=prefix.substring(0,prefix.length()-1)+":"+label;
            }
        }

        return result;
    }

    private String getShortForm(String str){
        int pos = str.indexOf("/");

        String result = "";

        while(pos!=-1){
            result += str.substring(pos+1,pos+1);

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
}
