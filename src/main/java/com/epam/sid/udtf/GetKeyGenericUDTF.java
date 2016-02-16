package com.epam.sid.udtf;

import com.epam.sid.idstore.IdStore;
import com.epam.sid.idstore.IdZooStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Generates table with syntetic IDs
 */
@Description(
        name = "Sequence generator",
        value = "_FUNC_(<sequence name>, ...) - Generate next ID for the sequence",
        extended = "Example:\n" +
                "  > SELECT _FUNC_(<sequence name>, A, B) FROM data;\n" +
                "Generates:\n" +
                "> 1,A1,B1\n" +
                "> ...\n" +
                "> n,An,Bn:\n"
)
public class GetKeyGenericUDTF extends GenericUDTF {
    private static final String IDColumnName = "id";
    private IdStore store = new IdZooStore();
    private static final Log log = LogFactory.getLog(GetKeyGenericUDTF.class.getName());

    /**
     * Calls before first processing
     * @param argOIs - columns list
     * @return output inspector
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        log.debug("initialize..");
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        fieldNames.add(IDColumnName);

        Iterator<? extends StructField> fieldIter = argOIs.getAllStructFieldRefs().iterator();
        if (fieldIter.hasNext()) {
            fieldIter.next();
        }
        while (fieldIter.hasNext()) {
            StructField field = fieldIter.next();
            fieldOIs.add(field.getFieldObjectInspector());
            fieldNames.add(field.getFieldName());
        }

        log.info("initialize completed with " + fieldOIs.size() + " records");
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * Processing one record
     * @param objects fields of the record
     * @throws HiveException
     */
    @Override
    public void process(Object[] objects) throws HiveException {
        log.debug("Start processing");
        final String sequenceName = objects.length == 0 ? "" : objects[0].toString();

        List o = new LinkedList();
        o.add(store.getNextId(sequenceName));

        for (int i = 1; i < objects.length; ++i) {
            o.add(objects[i]);
        }

        log.debug("Complete processing");
        forward(o);
    }

    /**
     * Calls after processing is finished
     * @throws HiveException
     */
    @Override
    public void close() {
        store.close();
    }
}
