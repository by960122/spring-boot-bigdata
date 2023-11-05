package com.example.hive.udf;

import java.util.UUID;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

class MyUuid extends GenericUDF {
    private PrimitiveObjectInspector input;

    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentLengthException {
        if (args.length == 0) {
            throw new UDFArgumentLengthException(UUID.randomUUID().toString());
        } else {
            input = (PrimitiveObjectInspector)args[0];
            assert input.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT;

            // 输出参数
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        }
    }

    public Object evaluate(DeferredObject[] args) throws HiveException {
        String uuid = UUID.randomUUID().toString();
        int i = (Integer)input.getPrimitiveJavaObject(args[0].get());
        if (i > uuid.length()) {
            return new Text(uuid);
        } else {
            return new Text(uuid.substring(0, i));
        }
    }

    public String getDisplayString(String[] args) {
        return "Here, write a nice description";
    }
}