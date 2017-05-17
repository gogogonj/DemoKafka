package streams;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by AI on 2017/5/17.
 */
public class Test {
    public static void main(String [] args){
        String need = "e,a,c,g";

        List<String> allList = new ArrayList<String>();
        allList.add("a");
        allList.add("b");
        allList.add("c");
        allList.add("d");
        allList.add("e");
        allList.add("f");

        String [] needs = need.split(",");
        for(int i = 0; i<needs.length; i++){
            int num = allList.indexOf(needs[i]);
            if(num!=-1){
                System.out.println(num);
            }
        }
    }
}
