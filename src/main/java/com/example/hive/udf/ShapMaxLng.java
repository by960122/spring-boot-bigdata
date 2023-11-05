package com.example.hive.udf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description: 取shapae字段最大的经度
 */
class ShapMaxLng extends GenericUDF {
    private PrimitiveObjectInspector input;

    public ObjectInspector initialize(ObjectInspector[] args) {
        input = (PrimitiveObjectInspector)args[0];
        assert input.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING;

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    public Object evaluate(DeferredObject[] args) throws HiveException {
        List<String> list = new ArrayList<>();
        String shape = (String)input.getPrimitiveJavaObject(args[0].get());
        String[] jwd = shape.replace("POLYGON  (( ", "").replace("MULTIPOLYGON  ((( ", "").replace(")", "")
            .replace("(", "").split(", ");
        for (String lng_lat : jwd) {
            String[] lng = lng_lat.split(" ");
            list.add(lng[0]);
        }
        return new Text(Collections.max(list));
    }

    public String getDisplayString(String[] strings) {
        return null;
    }
}
