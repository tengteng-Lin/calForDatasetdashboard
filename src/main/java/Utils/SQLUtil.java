package Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class SQLUtil {

    public static String getURIForId(int table_id,int dataset_local_id,int id){
        Connection connection = Utils.JdbcUtil.getConnection(Utils.GlobalVariances.REMOTE);
        String uri="";
        String sql = String.format("SELECT * FROM uri_label_id%d WHERE dataset_local_id=%d AND id = %d",table_id,dataset_local_id,id);
        try{
            PreparedStatement pst = connection.prepareStatement(sql);
            ResultSet rst = pst.executeQuery();
//        System.out.println(rst.);
            if(rst.next()){
                uri = rst.getString("uri");
//            System.out.println("label:"+label);
            }

            rst.close();pst.close();connection.close();
        }catch(Exception e){
            e.printStackTrace();
        }

        return uri;


    }

    public static String getLabelForId(int table_id,int dataset_local_id,int id){
        Connection connection = Utils.JdbcUtil.getConnection(Utils.GlobalVariances.REMOTE);
        String label="";
        String sql = String.format("SELECT * FROM uri_label_id%d WHERE dataset_local_id=%d AND id = %d",table_id,dataset_local_id,id);
        try{
            PreparedStatement pst = connection.prepareStatement(sql);
            ResultSet rst = pst.executeQuery();
//        System.out.println(rst.);
            if(rst.next()){
                label = rst.getString("label");
//            System.out.println("label:"+label);
            }

            rst.close();pst.close();connection.close();
        }catch(Exception e){
            e.printStackTrace();
        }

        return label;
    }
}
