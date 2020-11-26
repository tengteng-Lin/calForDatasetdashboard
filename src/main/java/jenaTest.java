import org.apache.jena.shared.PrefixMapping;

import java.util.Map;

public class jenaTest {
//    Node n;
//    static PrefixMapping.Factory prefixes = new PrefixMapping.Factory();
    static PrefixMapping prefixMapping = PrefixMapping.Factory.create();


    public static void main(String args[]){
        String test = prefixMapping.shortForm("http://webenemasuno.linkeddata.es/elviajero/resource/Point/POINT48.85666666666667_2");
        System.out.println(prefixMapping.numPrefixes());

    }
}
