import static java.lang.Math.toIntExact;


public class HashFunctions {

    /**
     * @author Yacine A.
     * Java Class where hashing functions are implemented
     * For theses functions we used the MurmurHash Algorithm created by Austin Appleby in 2008
     * The algorithm is an implemntation of the standard multiplicative hashing algorithm
     *  - Ha(Key)=(a*K mod W)/(W/M)
     * if we chose W and M as pow of 2 we get :
     *  - Ha(Key)=(aK mod 2^w)/2^(w-n)
     * This is special because arithmetic modulo 2^w is done by default in low-level programming languages and integer division by a power of 2 is simply a right-shift,
     * which we could write return (a*K) >>> (w-m);
     *
     * So the constant chosen to be used int the implementation bellow were found to:
     * - Avoid collision
     * - Have the best distribution of the hashed key
     * - Grantie idempotence
     */



    /**
     *
     * @param key the key to hash = value in hive column
     * @return the hashed value
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
     * @param key the key to hash = value in hive column
     * @return the hashed value
     *
     * If the value if key could be casted to an Int without overflow:
     * - we call the function hashInt
     * else
     * - We implement the algorithme to hash Long values
     *
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
