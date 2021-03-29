import org.apache.hadoop.hive.common.type.Date;

import static org.apache.parquet.hadoop.util.counters.BenchmarkCounter.getTime;

public class HashFunctions {

    static Integer hashInt(Integer x) {
        x ^= x >> 16; // x >> 16 equivalent x mod 2^16
        x *= 0x7feb352d;
        x ^= x >> 15; // x >> 15 equivalent x mod 2^15
        x *= 0x846ca68b;
        x ^= x >> 16;  // x >> 16 equivalent x mod 2^16
        return x;
    }
    static Long hashLong(long x) {
        x ^= x >> 16; // x >> 16 equivalent x mod 2^16
        x *= 0x7feb352d;
        x ^= x >> 15; // x >> 15 equivalent x mod 2^15
        x *= 0x846ca68b;
        x ^= x >> 16;
        return x;
    }

}
