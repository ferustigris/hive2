package com.epam.sid.udtf;

import com.google.protobuf.LazyStringList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * Generates new table with syntetic IDs
 */
public class GetKeyGenericUDTF9 extends GenericUDTF {
    private LongWritable currentID = new LongWritable(0);

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        fieldNames.add("ID");
        for (StructField argOI : argOIs.getAllStructFieldRefs()) {
            fieldOIs.add(argOI.getFieldObjectInspector());
            fieldNames.add("someelse");
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        if (objects.length < 1) {
            return;
        }
        //final String sequenceName =
        ArrayList o = new ArrayList();
        o.add(currentID);

        for (Object object : objects) {
            o.add(object);
        }

        currentID.set(currentID.get() + 1);
        forward(o);
    }

    @Override
    public void close() throws HiveException {

    }
}
