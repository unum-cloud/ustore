package com.unum.ukv;

import org.junit.Test;

import java.util.Arrays;

public class DataBaseSTL extends DataBase {
    static {
        try {
            System.load("/home/ishkhan/Code/UKV/build/libs/ukv/shared/stl/libukv.so");
        } catch (UnsatisfiedLinkError e) {
            String paths = System.getProperty("java.library.path");
            System.err.println("Native code library failed to load.\n" + e);
            System.err.println("Shared libraries must be placed at:" + paths + "\n");
            System.exit(1);
        }
    }

    @Test
    public void test() {
        Context ctx = new Context("");
        ctx.put(42, "hey".getBytes());
        assert Arrays.equals(ctx.get(42), "hey".getBytes()) : "Received wrong value";

        Transaction txn = ctx.transaction();
        txn.put("any", 42, "meaning of life".getBytes());
        assert Arrays.equals(txn.get("any", 42), "meaning of life".getBytes()) : "Wrong philosophy";
        txn.commit();
        assert Arrays.equals(ctx.get("any", 42), "meaning of life".getBytes()) : "Accepted wrong philosophy";

        ctx.close();
        System.out.println("Success!");
    }
}

