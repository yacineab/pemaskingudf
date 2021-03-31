import org.apache.hadoop.hive.common.type.Date;

import static java.lang.Math.toIntExact;
import static org.apache.parquet.hadoop.util.counters.BenchmarkCounter.getTime;

public class HashFunctions {

    /**
     *
     * @param key
     * @return
     */
    static Integer hashInt(Integer key) {
        key ^= key >>> 16; // x >> 16 equivalent x mod 2^16
        key *= 0x85ebca6b;
        key ^= key >>> 13; // x >> 15 equivalent x mod 2^13
        key *= 0xc2b2ae35;
        key ^= key >>> 16; // x >> 16 equivalent x mod 2^16
        return key;
    }

    /**
     *
     * @param key
     * @return
     */
    static Long hashLong(Long key) {
        long res;
        try {
            int keycast = toIntExact(key);
            res = hashInt(keycast);
        }
        catch (ArithmeticException e ){
            key += 0x9e3779b97f4a7c15L;
            key ^= (key >>> 30); // x >> 32 equivalent key mod 2^30
            key *= 0xbf58476d1ce4e5b9L;
            key ^= (key >>> 27); // x >> 32 equivalent key mod 2^27
            key *= 0x94d049bb133111ebL;
            key ^= (key >>> 31); // x >> 32 equivalent key mod 2^31
            res = key;
        }

        return res;

    }

}
