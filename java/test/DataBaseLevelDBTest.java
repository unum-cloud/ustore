import java.io.File;
import java.util.Arrays;

import org.junit.Test;

import com.unum.ukv.DataBaseLevelDB;

public class DataBaseLevelDBTest {
    static {
        DataBaseLevelDB.init();
    }

    public static void deleteDirectoryFiles(File directory) {
        if (directory.isDirectory()) {
            File[] contents = directory.listFiles();
            for (File f : contents)
                f.delete();
        }
    }

    @Test
    public void test() {
        String path = "./tmp/";
        deleteDirectoryFiles(new File(path));

        DataBaseLevelDB.Context ctx = new DataBaseLevelDB.Context(path);

        ctx.put(42, "hey".getBytes());
        assert Arrays.equals(ctx.get(42), "hey".getBytes()) : "Received wrong value";

        ctx.close();
        System.out.println("Success!");
    }
}
