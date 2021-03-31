import junit.framework.TestCase;
import org.junit.Test;


public class TestHashFunctions extends TestCase {

    /**
     * Test if hashInt hashes int value
     * @throws Exception
     */
    @Test
    public void testIntHash() throws Exception {
        int intKey=1121;
        HashFunctions hashFunctions = new HashFunctions();
        int res= hashFunctions.hashInt(intKey);
        assertEquals(167390031, res);
    }

    /**
     * Test if hashLong hashes long value
     * @throws Exception
     */
    @Test
    public void testLongHash() throws Exception {
        long longKey=115721L;
        long longKey2=115721499850231L;
        HashFunctions hashFunctions = new HashFunctions();
        long res = hashFunctions.hashLong(longKey);
        long res2 = hashFunctions.hashLong(longKey2);
        assertEquals(593177390, res);
        assertEquals(4751822681563970917L, res2);
    }

    /**
     * Test that we get the same result if we hash same value with hashInt or hashLong
     * @throws Exception
     */
    @Test
    public void testLongIntHash() throws Exception {
        int intLongKey=115721;
        HashFunctions hashFunctions = new HashFunctions();
        long resInt= hashFunctions.hashInt(intLongKey);
        long resLong= hashFunctions.hashLong((long) intLongKey);
        assertEquals(resInt, resLong);
    }

}
