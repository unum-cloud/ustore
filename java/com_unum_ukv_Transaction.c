#include "com_unum_ukv_Transaction.h"
#include "com_unum_ukv_Shared.h"

JNIEXPORT void JNICALL Java_com_unum_ukv_Transaction_put( //
    JNIEnv* env_java,
    jobject txn_java,
    jstring column_java,
    jlong key_java,
    jbyteArray value_java) {

    // Extract the raw pointer to underlying data
    // https://docs.oracle.com/en/java/javase/13/docs/specs/jni/functions.html
    jboolean value_is_copy_java = JNI_FALSE;
    jbyte* value_ptr_java = (*env_java)->GetByteArrayElements(env_java, value_java, &value_is_copy_java);
    jsize value_len_java = (*env_java)->GetArrayLength(env_java, value_java);

    // Cast everything to our types
    ukv_t db_ptr_c = db_ptr(env_java, txn_java);
    ukv_txn_t txn_ptr_c = txn_ptr(env_java, txn_java);
    ukv_key_t key_c = (ukv_key_t)key_java;
    ukv_val_ptr_t value_ptr_c = (ukv_val_ptr_t)value_ptr_java;
    ukv_val_len_t value_len_c = (ukv_val_len_t)value_len_java;
    ukv_options_write_t options_c = NULL;
    ukv_error_t error_c = NULL;

    ukv_write( //
        db_ptr_c,
        txn_ptr_c,
        &key_c,
        1,
        NULL,
        options_c,
        &value_ptr_c,
        &value_len_c,
        &error_c);

    if (value_is_copy_java == JNI_TRUE)
        (*env_java)->ReleaseByteArrayElements(env_java, value_java, value_ptr_java, 0);
    forward_error(env_java, error_c);
}

JNIEXPORT jboolean JNICALL Java_com_unum_ukv_Transaction_containsKey( //
    JNIEnv* env_java,
    jobject txn_java,
    jstring column_java,
    jlong key_java) {

    ukv_t db_ptr_c = db_ptr(env_java, txn_java);
    ukv_txn_t txn_ptr_c = txn_ptr(env_java, txn_java);
    ukv_key_t key_c = (ukv_key_t)key_java;
    ukv_options_read_t options_c = NULL;
    ukv_arena_ptr_t arena_c = NULL;
    size_t arena_len_c = 0;
    ukv_val_len_t value_len_c = 0;
    ukv_error_t error_c = NULL;

    ukv_read( //
        db_ptr_c,
        txn_ptr_c,
        &key_c,
        1,
        NULL,
        options_c,
        &arena_c,
        &arena_len_c,
        NULL,
        &value_len_c,
        &error_c);

    if (arena_c)
        ukv_arena_free(db_ptr_c, arena_c, arena_len_c);
    if (forward_error(env_java, error_c))
        return false;

    return value_len_c != 0;
}

JNIEXPORT jbyteArray JNICALL Java_com_unum_ukv_Transaction_get( //
    JNIEnv* env_java,
    jobject txn_java,
    jstring column_java,
    jlong key_java) {

    jbyteArray result_java = NULL;
    // For small lookups its jenerally cheaper to allocate new Java buffers
    // and copy the data there:
    // https://stackoverflow.com/a/28799276
    // https://stackoverflow.com/a/4694102
    // result_java = (*env_java)->NewDirectByteBuffer(env_java, void* address, jlong capacity);

    ukv_t db_ptr_c = db_ptr(env_java, txn_java);
    ukv_txn_t txn_ptr_c = txn_ptr(env_java, txn_java);
    ukv_key_t key_c = (ukv_key_t)key_java;
    ukv_options_read_t options_c = NULL;
    ukv_arena_ptr_t arena_c = NULL;
    size_t arena_len_c = 0;
    ukv_val_ptr_t value_ptr_c = NULL;
    ukv_val_len_t value_len_c = 0;
    ukv_error_t error_c = NULL;

    ukv_read( //
        db_ptr_c,
        txn_ptr_c,
        &key_c,
        1,
        NULL,
        options_c,
        &arena_c,
        &arena_len_c,
        &value_ptr_c,
        &value_len_c,
        &error_c);

    if (value_len_c) {
        result_java = (*env_java)->NewByteArray(env_java, value_len_c);
        if (result_java)
            (*env_java)->SetByteArrayRegion(env_java, result_java, 0, value_len_c, (jbyte const*)value_ptr_c);
    }

    if (arena_c)
        ukv_arena_free(db_ptr_c, arena_c, arena_len_c);

    forward_error(env_java, error_c);
    return result_java;
}

JNIEXPORT jbyteArray JNICALL Java_com_unum_ukv_Transaction_remove( //
    JNIEnv* env_java,
    jobject txn_java,
    jstring column_java,
    jlong key_java) {

    ukv_t db_ptr_c = db_ptr(env_java, txn_java);
    ukv_txn_t txn_ptr_c = txn_ptr(env_java, txn_java);
    ukv_key_t key_c = (ukv_key_t)key_java;
    ukv_val_ptr_t value_ptr_c = NULL;
    ukv_val_len_t value_len_c = 0;
    ukv_options_write_t options_c = NULL;
    ukv_error_t error_c = NULL;

    ukv_write( //
        db_ptr_c,
        txn_ptr_c,
        &key_c,
        1,
        NULL,
        options_c,
        &value_ptr_c,
        &value_len_c,
        &error_c);
    forward_error(env_java, error_c);
}

JNIEXPORT void JNICALL Java_com_unum_ukv_Transaction_rollback( //
    JNIEnv* env_java,
    jobject txn_java) {

    ukv_t db_ptr_c = db_ptr(env_java, txn_java);
    ukv_txn_t txn_ptr_c = txn_ptr(env_java, txn_java);
    ukv_error_t error_c = NULL;

    ukv_txn_begin(db_ptr_c, 0, &txn_ptr_c, &error_c);
    forward_error(env_java, error_c);
}

JNIEXPORT jboolean JNICALL Java_com_unum_ukv_Transaction_commit( //
    JNIEnv* env_java,
    jobject txn_java) {

    ukv_txn_t txn_ptr_c = txn_ptr(env_java, txn_java);
    ukv_options_write_t options_c = NULL;
    ukv_error_t error_c = NULL;

    ukv_txn_commit(txn_ptr_c, options_c, &error_c);
    return error_c ? JNI_FALSE : JNI_TRUE;
}