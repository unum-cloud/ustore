#include "com_unum_ukv_DataBase.h"
#include "ukv.h"

JNIEXPORT void JNICALL Java_com_unum_ukv_DataBase_open(JNIEnv* env, jobject db, jstring config) {
    jclass db_class = (*env)->GetObjectClass(env, db);
    jfieldID db_cptr_id = (*env)->GetFieldID(env, db_class, "nativeAddress", "I");
    if (db_cptr_id == NULL)
        return;

    long int db_cptr_int = (*env)->GetIntField(env, db, db_cptr_id);
    char const* config_cstr = (*env)->GetStringUTFChars(env, config, NULL);

    ukv_t db_cptr = NULL;
    ukv_error_t error = NULL;
    ukv_open(config_cstr, &db, &error);

    db_cptr_int = static_cast<long int>(db_cptr);
    (*env)->SetIntField(env, db, db_cptr_id, db_cptr_int);
    (*env)->ReleaseStringUTFChars(env, config, config_cstr);
}

JNIEXPORT void JNICALL Java_com_unum_ukv_DataBase_put(JNIEnv* env, jobject db, jlong key, jbyteArray value) {
    // https://docs.oracle.com/en/java/javase/13/docs/specs/jni/functions.html
    jbyte* ptr = (*env)->GetByteArrayElements(env, value, NULL);
    jsize len = (*env)->GetArrayLength(env, value);

    (*env)->ReleaseByteArrayElements(env, value, ptr, 0);
}

JNIEXPORT jboolean JNICALL Java_com_unum_ukv_DataBase_containsKey(JNIEnv* env, jobject db, jlong key) {
}

JNIEXPORT jbyteArray JNICALL Java_com_unum_ukv_DataBase_get(JNIEnv* env, jobject db, jlong key) {

    // jobject result = (*env)->NewDirectByteBuffer(env, void* address, jlong capacity);
    // https://stackoverflow.com/a/4694102
}

JNIEXPORT jbyteArray JNICALL Java_com_unum_ukv_DataBase_remove__J(JNIEnv* env, jobject db, jlong key) {
}

JNIEXPORT void JNICALL Java_com_unum_ukv_DataBase_clear(JNIEnv* env, jobject db) {
}