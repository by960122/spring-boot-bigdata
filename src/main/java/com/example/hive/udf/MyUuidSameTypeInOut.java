package com.example.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description: 传入传出参数类型一样的版本
 */
class MyUuidSameTypeInOut extends GenericUDF {
    private WritableIntObjectInspector input;

    public ObjectInspector initialize(ObjectInspector[] args) {
        input = (WritableIntObjectInspector)args[0];
        return input;
    }

    public Object evaluate(DeferredObject[] args) throws HiveException {
        String uuid = "123456789";
        Object o = args[0].get();

        return uuid.substring(0, input.get(o));
    }

    public String getDisplayString(String[] args) {
        return "Here, write a nice description";
    }
}