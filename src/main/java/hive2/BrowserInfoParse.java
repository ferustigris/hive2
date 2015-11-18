package hive2;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

@Description(
        name = "browser_info_parser",
        value = "_FUNC_(str) - Converts a browser info string to separated fields",
        extended = "Example:\n" +
                "  > SELECT browser_info_parser(...) FROM data d;\n" +
                "  STEPHEN KING"
)
public class BrowserInfoParse extends GenericUDTF {

    @Override
    public void process(Object[] objects) throws HiveException {
        String s1[] = {"str1"};
        String s2[] = {"str1"};
        for (Object object : objects) {
            System.out.println("Obj:" + object.toString());
        }
        forward(s1);
        forward(s2);
    }

    @Override
    public void close() throws HiveException {

    }
}
