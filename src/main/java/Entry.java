import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;

import java.nio.file.Paths;

public class Entry {
    public static void main(String args[]){
        ToolGuy toolGuy = new ToolGuy();
//////        toolGuy.getMaxOutdegreeNode(2,1);
//        toolGuy.getNamespace(2,1);
        for (int i=3701;i<=9629;i++){
//            toolGuy.typeID=-1;
            toolGuy.getNamespace(i); //12Huo13
//            toolGuy.getMaxOutdegreeNode(2,i);
            System.out.println("dataset "+i+" end!");
        }

//        testSearch();

    }

    public static void testSearch(){
        try{
            Directory directory = MMapDirectory.open(Paths.get("D:\\Index\\Namespace\\2\\1\\"));
            IndexReader reader = DirectoryReader.open(directory);

            for(int i=0;i<reader.maxDoc();i++){
                Document doc = reader.document(i);
                System.out.println(doc.get("vocabulary")+"\t"+doc.get("prefix"));
            }
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
