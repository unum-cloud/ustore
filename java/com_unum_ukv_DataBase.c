#include "com_unum_ukv_DataBase.h"
#include "ukv.h"

jfieldID find_db_ptr_field(JNIEnv* env_java, jobject db_java) {
    static jfieldID db_ptr_field = NULL;
    if (!db_ptr_field) {
        // https://docs.oracle.com/javase/8/docs/technotes/guides/jni/spec/types.html
        // https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
        jclass db_class = (*env_java)->GetObjectClass(env_java, db_java);
        db_ptr_field = (*env_java)->GetFieldID(env_java, db_class, "nativeAddress", "J");
    }
    return db_ptr_field;
}

ukv_t db_ptr(JNIEnv* env_java, jobject db_java) {
    jfieldID db_ptr_field = find_db_ptr_field(env_java, db_java);
    long int db_ptr_java = (*env_java)->GetLongField(env_java, db_java, db_ptr_field);
    return (ukv_t)db_ptr_java;
}

/**
 * @return true  If error was detected.
 * @return false If no error appeared.
 */
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

JNIEXPORT void JNICALL Java_com_unum_ukv_DataBase_open(JNIEnv* env_java, jobject db_java, jstring config_java) {

    ukv_t db_ptr_c = db_ptr(env_java, db_java);
    if (db_ptr_c)
        // The DB is already initialized
        return;

    // Temporarily copy the contents of the passed configuration string
    char const* config_c = (*env_java)->GetStringUTFChars(env_java, config_java, NULL);
    ukv_error_t error_c = NULL;

    ukv_open(config_c, &db_ptr_c, &error_c);
    (*env_java)->ReleaseStringUTFChars(env_java, config_java, config_c);
    if (forward_error(env_java, error_c))
        return;

    jfieldID db_ptr_field = find_db_ptr_field(env_java, db_java);
    (*env_java)->SetLongField(env_java, db_java, db_ptr_field, (long int)db_ptr_c);
}

JNIEXPORT void JNICALL Java_com_unum_ukv_DataBase_close_1(JNIEnv* env_java, jobject db_java) {

    ukv_t db_ptr_c = db_ptr(env_java, db_java);
    if (!db_ptr_c)
        // The DB is already closed
        return;

    // Overwrite the field first, to avoid multiple deallocations
    jfieldID db_ptr_field = find_db_ptr_field(env_java, db_java);
    (*env_java)->SetLongField(env_java, db_java, db_ptr_field, (long int)0);

    // Then actually dealloc
    ukv_free(db_ptr_c);
}

JNIEXPORT void JNICALL Java_com_unum_ukv_DataBase_put(JNIEnv* env_java,
                                                      jobject db_java,
                                                      jlong key_java,
                                                      jbyteArray value_java) {

    // Extract the raw pointer to underlying data
    // https://docs.oracle.com/en/java/javase/13/docs/specs/jni/functions.html
    jboolean value_is_copy_java = JNI_FALSE;
    jbyte* value_ptr_java = (*env_java)->GetByteArrayElements(env_java, value_java, &value_is_copy_java);
    jsize value_len_java = (*env_java)->GetArrayLength(env_java, value_java);

    // Cast everything to our types
    ukv_t db_ptr_c = db_ptr(env_java, db_java);
    ukv_key_t key_c = (ukv_key_t)key_java;
    ukv_val_ptr_t value_ptr_c = (ukv_val_ptr_t)value_ptr_java;
    ukv_val_len_t value_len_c = (ukv_val_len_t)value_len_java;
    ukv_options_write_t options_c = NULL;
    ukv_error_t error_c = NULL;

    ukv_write(db_ptr_c, NULL, &key_c, 1, NULL, options_c, &value_ptr_c, &value_len_c, &error_c);
    if (value_is_copy_java == JNI_TRUE)
        (*env_java)->ReleaseByteArrayElements(env_java, value_java, value_ptr_java, 0);
    forward_error(env_java, error_c);
}

JNIEXPORT jboolean JNICALL Java_com_unum_ukv_DataBase_containsKey(JNIEnv* env_java, jobject db_java, jlong key_java) {

    ukv_t db_ptr_c = db_ptr(env_java, db_java);
    ukv_key_t key_c = (ukv_key_t)key_java;
    ukv_options_read_t options_c = NULL;
    ukv_arena_ptr_t arena_c = NULL;
    size_t arena_len_c = 0;
    ukv_val_len_t value_len_c = 0;
    ukv_error_t error_c = NULL;

    ukv_read(db_ptr_c, NULL, &key_c, 1, NULL, options_c, &arena_c, &arena_len_c, NULL, &value_len_c, &error_c);
    if (arena_c)
        ukv_arena_free(db_ptr_c, arena_c, arena_len_c);
    if (forward_error(env_java, error_c))
        return false;

    return value_len_c != 0;
}

JNIEXPORT jbyteArray JNICALL Java_com_unum_ukv_DataBase_get(JNIEnv* env_java, jobject db_java, jlong key_java) {

    jbyteArray result_java = NULL;
    // For small lookups its jenerally cheaper to allocate new Java buffers
    // and copy the data there:
    // https://stackoverflow.com/a/28799276
    // https://stackoverflow.com/a/4694102
    // result_java = (*env_java)->NewDirectByteBuffer(env_java, void* address, jlong capacity);

    ukv_t db_ptr_c = db_ptr(env_java, db_java);
    ukv_key_t key_c = (ukv_key_t)key_java;
    ukv_options_read_t options_c = NULL;
    ukv_arena_ptr_t arena_c = NULL;
    size_t arena_len_c = 0;
    ukv_val_ptr_t value_ptr_c = NULL;
    ukv_val_len_t value_len_c = 0;
    ukv_error_t error_c = NULL;

    ukv_read(db_ptr_c, NULL, &key_c, 1, NULL, options_c, &arena_c, &arena_len_c, &value_ptr_c, &value_len_c, &error_c);
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

JNIEXPORT jbyteArray JNICALL Java_com_unum_ukv_DataBase_remove__J(JNIEnv* env_java, jobject db_java, jlong key_java) {

    ukv_t db_ptr_c = db_ptr(env_java, db_java);
    ukv_key_t key_c = (ukv_key_t)key_java;
    ukv_val_ptr_t value_ptr_c = NULL;
    ukv_val_len_t value_len_c = 0;
    ukv_options_write_t options_c = NULL;
    ukv_error_t error_c = NULL;

    ukv_write(db_ptr_c, NULL, &key_c, 1, NULL, options_c, &value_ptr_c, &value_len_c, &error_c);
    forward_error(env_java, error_c);
}

JNIEXPORT void JNICALL Java_com_unum_ukv_DataBase_clear(JNIEnv* env_java, jobject db_java) {

    ukv_t db_ptr_c = db_ptr(env_java, db_java);
    ukv_error_t error_c = NULL;

    ukv_column_remove(db_ptr_c, NULL, &error_c);
    forward_error(env_java, error_c);
}