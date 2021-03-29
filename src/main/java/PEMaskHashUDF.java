import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class PEMaskHashUDF extends BaseMaskUDF {
    public static final String UDF_NAME = "mask_hash";

    public PEMaskHashUDF() {
        super(new MaskHashTransformer(), UDF_NAME);
    }
}
class MaskHashTransformer extends AbstractTransformer {
    @Override
    public void init(ObjectInspector[] arguments, int startIdx) {
    }

    @Override
    String transform(final String value) {
        return DigestUtils.sha256Hex(value);
    }

    @Override
    Byte transform(final Byte value) {
        return null;
    }

    @Override
    Short transform(final Short value) {
        return null;
    }

    @Override
    Integer transform(final Integer value) {
        return HashFunctions.hashInt(value);

    }

    @Override
    Long transform(final Long value) {
        return HashFunctions.hashLong(value);
    }

    @Override
    Date transform(final Date value) {
        return null;
    }
}
