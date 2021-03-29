import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseMaskUDF extends GenericUDF {
    private static final Logger LOG = LoggerFactory.getLogger(BaseMaskUDF.class);

    final protected AbstractTransformer  transformer;
    final protected String               displayName;
    protected AbstractTransformerAdapter transformerAdapter = null;

    protected BaseMaskUDF(AbstractTransformer transformer, String displayName) {
        this.transformer = transformer;
        this.displayName = displayName;
    }

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        LOG.debug("==> BaseMaskUDF.initialize()");

        checkArgPrimitive(arguments, 0); // first argument is the column to be transformed

        PrimitiveObjectInspector columnType = ((PrimitiveObjectInspector) arguments[0]);

        transformer.init(arguments, 1);

        transformerAdapter = AbstractTransformerAdapter.getTransformerAdapter(columnType, transformer);

        ObjectInspector ret = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(columnType.getPrimitiveCategory());

        LOG.debug("<== BaseMaskUDF.initialize()");

        return ret;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object ret = transformerAdapter.getTransformedWritable(arguments[0]);

        return ret;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(displayName, children);
    }
}
