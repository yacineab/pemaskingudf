import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

abstract class AbstractTransformer {
    /**
     * Initialzie the transformer object
     * @param arguments arguments given to GenericUDF.initialzie()
     * @param startIdx index into array, from which the transformer should read values
     */
    abstract void init(ObjectInspector[] arguments, int startIdx);

    /**
     * Transform a String value
     * @param value value to transform
     * @return transformed value
     */
    abstract String transform(String value);

    /**
     * Transform a Byte value
     * @param value value to transform
     * @return transformed value
     */
    abstract Byte transform(Byte value);

    /**
     * Transform a Short value
     * @param value value to transform
     * @return transformed value
     */
    abstract Short transform(Short value);

    /**
     * Transform a Integer value
     * @param value value to transform
     * @return transformed value
     */
    abstract Integer transform(Integer value);

    /**
     * Transform a Long value
     * @param value value to transform
     * @return transformed value
     */
    abstract Long transform(Long value);

    /**
     * Transform a Date value
     * @param value value to transform
     * @return transformed value
     */
    abstract Date transform(Date value);
}
