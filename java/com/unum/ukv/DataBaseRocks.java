package com.unum.ukv;

public class DataBaseRocks extends DataBase {
    public static void init() {
        try {
            System.load("ukv/rocks/libukv.so");
        } catch (UnsatisfiedLinkError e) {
            String paths = System.getProperty("java.library.path");
            System.err.println("Native code library failed to load.\n" + e);
            System.err.println("Shared libraries must be placed at:" + paths + "\n");
            System.exit(1);
        }
    }
}
