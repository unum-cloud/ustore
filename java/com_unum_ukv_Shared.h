#include <jni.h>

#ifndef _Included_com_unum_ukv_Shared
#define _Included_com_unum_ukv_Shared
#ifdef __cplusplus
extern "C" {
#endif

#include "ukv.h"

jfieldID find_field_database_address(JNIEnv* env_java, jobject txn_java);

jfieldID find_field_transaction_address(JNIEnv* env_java, jobject txn_java);

ukv_t db_ptr(JNIEnv* env_java, jobject txn_java);

ukv_txn_t txn_ptr(JNIEnv* env_java, jobject txn_java);

/**
 * @return true  If error was detected.
 * @return false If no error appeared.
 */
bool forward_error(JNIEnv* env_java, ukv_error_t error_c);

#ifdef __cplusplus
}
#endif
#endif
