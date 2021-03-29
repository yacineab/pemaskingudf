import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

abstract class AbstractTransformerAdapter {
    final AbstractTransformer transformer;

    AbstractTransformerAdapter(AbstractTransformer transformer) {
        this.transformer = transformer;
    }

    abstract Object getTransformedWritable(GenericUDF.DeferredObject value) throws HiveException;

    static AbstractTransformerAdapter getTransformerAdapter(PrimitiveObjectInspector columnType, AbstractTransformer transformer) {
        final AbstractTransformerAdapter ret;

        switch(columnType.getPrimitiveCategory()) {
            case STRING:
                ret = new StringTransformerAdapter((StringObjectInspector)columnType, transformer);
                break;

            case CHAR:
                ret = new HiveCharTransformerAdapter((HiveCharObjectInspector)columnType, transformer);
                break;

            case VARCHAR:
                ret = new HiveVarcharTransformerAdapter((HiveVarcharObjectInspector)columnType, transformer);
                break;

            case BYTE:
                ret = new ByteTransformerAdapter((ByteObjectInspector)columnType, transformer);
                break;

            case SHORT:
                ret = new ShortTransformerAdapter((ShortObjectInspector)columnType, transformer);
                break;

            case INT:
                ret = new IntegerTransformerAdapter((IntObjectInspector)columnType, transformer);
                break;

            case LONG:
                ret = new LongTransformerAdapter((LongObjectInspector)columnType, transformer);
                break;

            case DATE:
                ret = new DateTransformerAdapter((DateObjectInspector)columnType, transformer);
                break;

            default:
                ret = new UnsupportedDatatypeTransformAdapter(columnType, transformer);
                break;
        }

        return ret;
    }
}
class ByteTransformerAdapter extends AbstractTransformerAdapter {
    final ByteObjectInspector columnType;
    final ByteWritable writable;

    public ByteTransformerAdapter(ByteObjectInspector columnType, AbstractTransformer transformer) {
        this(columnType, transformer, new ByteWritable());
    }

    public ByteTransformerAdapter(ByteObjectInspector columnType, AbstractTransformer transformer, ByteWritable writable) {
        super(transformer);

        this.columnType = columnType;
        this.writable   = writable;
    }

    @Override
    public Object getTransformedWritable(GenericUDF.DeferredObject object) throws HiveException {
        Byte value = (Byte)columnType.getPrimitiveJavaObject(object.get());

        if(value != null) {
            Byte transformedValue = transformer.transform(value);

            if(transformedValue != null) {
                writable.set(transformedValue);

                return writable;
            }
        }

        return null;
    }
}

class DateTransformerAdapter extends AbstractTransformerAdapter {
    final DateObjectInspector columnType;
    final DateWritableV2 writable;

    public DateTransformerAdapter(DateObjectInspector columnType, AbstractTransformer transformer) {
        this(columnType, transformer, new DateWritableV2());
    }

    public DateTransformerAdapter(DateObjectInspector columnType, AbstractTransformer transformer, DateWritableV2 writable) {
        super(transformer);

        this.columnType = columnType;
        this.writable   = writable;
    }

    @Override
    public Object getTransformedWritable(GenericUDF.DeferredObject object) throws HiveException {
        Date value = columnType.getPrimitiveJavaObject(object.get());

        if(value != null) {
            Date transformedValue = transformer.transform(value);

            if(transformedValue != null) {
                writable.set(transformedValue);

                return writable;
            }
        }

        return null;
    }
}

class HiveCharTransformerAdapter extends AbstractTransformerAdapter {
    final HiveCharObjectInspector columnType;
    final HiveCharWritable writable;

    public HiveCharTransformerAdapter(HiveCharObjectInspector columnType, AbstractTransformer transformer) {
        this(columnType, transformer, new HiveCharWritable());
    }

    public HiveCharTransformerAdapter(HiveCharObjectInspector columnType, AbstractTransformer transformer, HiveCharWritable writable) {
        super(transformer);

        this.columnType = columnType;
        this.writable   = writable;
    }

    @Override
    public Object getTransformedWritable(GenericUDF.DeferredObject object) throws HiveException {
        HiveChar value = columnType.getPrimitiveJavaObject(object.get());

        if(value != null) {
            String transformedValue = transformer.transform(value.getValue());

            if(transformedValue != null) {
                writable.set(transformedValue);

                return writable;
            }
        }

        return null;
    }
}

class HiveVarcharTransformerAdapter extends AbstractTransformerAdapter {
    final HiveVarcharObjectInspector columnType;
    final HiveVarcharWritable writable;

    public HiveVarcharTransformerAdapter(HiveVarcharObjectInspector columnType, AbstractTransformer transformer) {
        this(columnType, transformer, new HiveVarcharWritable());
    }

    public HiveVarcharTransformerAdapter(HiveVarcharObjectInspector columnType, AbstractTransformer transformer, HiveVarcharWritable writable) {
        super(transformer);

        this.columnType = columnType;
        this.writable   = writable;
    }

    @Override
    public Object getTransformedWritable(GenericUDF.DeferredObject object) throws HiveException {
        HiveVarchar value = columnType.getPrimitiveJavaObject(object.get());

        if(value != null) {
            String transformedValue = transformer.transform(value.getValue());

            if(transformedValue != null) {
                writable.set(transformedValue);

                return writable;
            }
        }

        return null;
    }
}

class IntegerTransformerAdapter extends AbstractTransformerAdapter {
    final IntObjectInspector columnType;
    final IntWritable writable;

    public IntegerTransformerAdapter(IntObjectInspector columnType, AbstractTransformer transformer) {
        this(columnType, transformer, new IntWritable());
    }

    public IntegerTransformerAdapter(IntObjectInspector columnType, AbstractTransformer transformer, IntWritable writable) {
        super(transformer);

        this.columnType = columnType;
        this.writable   = writable;
    }

    @Override
    public Object getTransformedWritable(GenericUDF.DeferredObject object) throws HiveException {
        Integer value = (Integer)columnType.getPrimitiveJavaObject(object.get());

        if(value != null) {
            Integer transformedValue = transformer.transform(value);

            if(transformedValue != null) {
                writable.set(transformedValue);

                return writable;
            }
        }

        return null;
    }
}

class LongTransformerAdapter extends AbstractTransformerAdapter {
    final LongObjectInspector columnType;
    final LongWritable writable;

    public LongTransformerAdapter(LongObjectInspector columnType, AbstractTransformer transformer) {
        this(columnType, transformer, new LongWritable());
    }

    public LongTransformerAdapter(LongObjectInspector columnType, AbstractTransformer transformer, LongWritable writable) {
        super(transformer);

        this.columnType = columnType;
        this.writable   = writable;
    }

    @Override
    public Object getTransformedWritable(GenericUDF.DeferredObject object) throws HiveException {
        Long value = (Long)columnType.getPrimitiveJavaObject(object.get());

        if(value != null) {
            Long transformedValue = transformer.transform(value);

            if(transformedValue != null) {
                writable.set(transformedValue);

                return writable;
            }
        }

        return null;
    }
}

class ShortTransformerAdapter extends AbstractTransformerAdapter {
    final ShortObjectInspector columnType;
    final ShortWritable writable;

    public ShortTransformerAdapter(ShortObjectInspector columnType, AbstractTransformer transformer) {
        this(columnType, transformer, new ShortWritable());
    }

    public ShortTransformerAdapter(ShortObjectInspector columnType, AbstractTransformer transformer, ShortWritable writable) {
        super(transformer);

        this.columnType = columnType;
        this.writable   = writable;
    }

    @Override
    public Object getTransformedWritable(GenericUDF.DeferredObject object) throws HiveException {
        Short value = (Short)columnType.getPrimitiveJavaObject(object.get());

        if(value != null) {
            Short transformedValue = transformer.transform(value);

            if(transformedValue != null) {
                writable.set(transformedValue);

                return writable;
            }
        }

        return null;
    }
}

class StringTransformerAdapter extends AbstractTransformerAdapter {
    final StringObjectInspector columnType;
    final Text writable;

    public StringTransformerAdapter(StringObjectInspector columnType, AbstractTransformer transformer) {
        this(columnType, transformer, new Text());
    }

    public StringTransformerAdapter(StringObjectInspector columnType, AbstractTransformer transformer, Text writable) {
        super(transformer);

        this.columnType = columnType;
        this.writable   = writable;
    }

    @Override
    public Object getTransformedWritable(GenericUDF.DeferredObject object) throws HiveException {
        String value = columnType.getPrimitiveJavaObject(object.get());

        if(value != null) {
            String transformedValue = transformer.transform(value);

            if(transformedValue != null) {
                writable.set(transformedValue);

                return writable;
            }
        }

        return null;
    }
}

class UnsupportedDatatypeTransformAdapter extends AbstractTransformerAdapter {
    final PrimitiveObjectInspector columnType;

    public UnsupportedDatatypeTransformAdapter(PrimitiveObjectInspector columnType, AbstractTransformer transformer) {
        super(transformer);

        this.columnType = columnType;
    }

    @Override
    public Object getTransformedWritable(GenericUDF.DeferredObject object) throws HiveException {
        return null;
    }
}
