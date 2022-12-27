
import com.unum.ukv.DataBaseLevelDB;
import org.junit.Test;

import java.util.Arrays;

public class DataBaseLevelDBTest {
    static {
        DataBaseLevelDB.init();
    }
    @Test
    public void test() {
        DataBaseLevelDB.Context ctx = new DataBaseLevelDB.Context("./tmp");
        ctx.put(42, "hey".getBytes());
        assert Arrays.equals(ctx.get(42), "hey".getBytes()) : "Received wrong value";

        ctx.close();
        System.out.println("Success!");
    }
}
