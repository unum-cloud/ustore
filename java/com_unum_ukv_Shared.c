#include "com_unum_ukv_Shared.h"

jfieldID find_field_database_address(JNIEnv* env_java, jobject txn_java) {
    static jfieldID db_ptr_field = NULL;
    if (!db_ptr_field) {
        // https://docs.oracle.com/javase/8/docs/technotes/guides/jni/spec/types.html
        // https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
        jclass db_class = (*env_java)->GetObjectClass(env_java, txn_java);
        db_ptr_field = (*env_java)->GetFieldID(env_java, db_class, "databaseAddress", "J");
    }
    return db_ptr_field;
}

jfieldID find_field_transaction_address(JNIEnv* env_java, jobject txn_java) {
    static jfieldID db_ptr_field = NULL;
    if (!db_ptr_field) {
        // https://docs.oracle.com/javase/8/docs/technotes/guides/jni/spec/types.html
        // https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
        jclass db_class = (*env_java)->GetObjectClass(env_java, txn_java);
        db_ptr_field = (*env_java)->GetFieldID(env_java, db_class, "transactionAddress", "J");
    }
    return db_ptr_field;
}

ukv_t db_ptr(JNIEnv* env_java, jobject txn_java) {
    jfieldID db_ptr_field = find_field_database_address(env_java, txn_java);
    long int db_ptr_java = (*env_java)->GetLongField(env_java, txn_java, db_ptr_field);
    return (ukv_t)db_ptr_java;
}

ukv_txn_t txn_ptr(JNIEnv* env_java, jobject txn_java) {
    jfieldID txn_ptr_field = find_field_transaction_address(env_java, txn_java);
    long int txn_ptr_java = (*env_java)->GetLongField(env_java, txn_java, txn_ptr_field);
    return (ukv_txn_t)txn_ptr_java;
}

bool forward_error(JNIEnv* env_java, ukv_error_t error_c) {
    if (!error_c)
        return false;

    // Error handling in JNI:
    // https://stackoverflow.com/a/15289742
    jclass error_java = (*env_java)->FindClass(env_java, "java/lang/Error");
    if (error_java)
        (*env_java)->ThrowNew(env_java, error_java, error_c);

    ukv_error_free(error_c);
    return true;
}