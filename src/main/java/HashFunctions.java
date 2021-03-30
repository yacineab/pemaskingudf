import org.apache.hadoop.hive.common.type.Date;

import static org.apache.parquet.hadoop.util.counters.BenchmarkCounter.getTime;

public class HashFunctions {

    static Integer hashInt(Integer x) {
        x ^= x >>> 16; // x >> 16 equivalent x mod 2^16
        x *= 0x85ebca6b;
        x ^= x >>> 13; // x >> 15 equivalent x mod 2^13
        x *= 0xc2b2ae35;
        x ^= x >>> 16; // x >> 16 equivalent x mod 2^16
        return x;
    }
    static Long hashLong(long x) {
        x += 0x9e3779b97f4a7c15L;
        x ^= (x >>> 30); // x >> 32 equivalent x mod 2^30
        x *= 0xbf58476d1ce4e5b9L;
        x ^= (x >>> 27); // x >> 32 equivalent x mod 2^27
        x *= 0x94d049bb133111ebL;
        x ^= (x >>> 31); // x >> 32 equivalent x mod 2^31
        return x;

    }

}
