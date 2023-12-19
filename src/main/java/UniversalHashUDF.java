import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@Description(name = "universalhashudf",
        value = "returns hash of the given value",
        extended = "Examples:\n "
                + "  universalhashudf(value)\n "
                + "Arguments:\n "
                + "  value - value to mask. Supported types: STRING, VARCHAR, CHAR, INT, BIGINT"
)

public class UniversalHashUDF extends BaseMaskUDF {
    public static final String UDF_NAME = "universalhashudf";

    public UniversalHashUDF() {
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

    /**
     *
     * @param value value to transform
     * @return hashedInt of the Value
     */
    @Override
    Integer transform(final Integer value) {
        return HashFunctions.hashInt(value);

    }

    /**
     *
     * @param value value to transform
     * @return hashedLong of the Value
     */
    @Override
    Long transform(final Long value) {
        return HashFunctions.hashLong(value);
    }

    @Override
    Date transform(final Date value) {
        return null;
    }
}
