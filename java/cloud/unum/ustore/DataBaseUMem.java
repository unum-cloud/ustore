package cloud.unum.ustore;


public class DataBaseUMem extends DataBase {
    public static void init() {
        loadLibrary("umem");
    }
}

