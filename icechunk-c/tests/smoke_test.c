/*
 * Smoke test for the icechunk C API.
 *
 * Exercises: storage creation, store open, set metadata, set chunk,
 * exists, get (with roundtrip verify), list, delete, free.
 *
 * Build (from repo root):
 *   cargo build -p icechunk-c
 *   cc -o icechunk-c/tests/smoke_test \
 *      icechunk-c/tests/smoke_test.c \
 *      -I icechunk-c/include \
 *      -L target/debug -licechunk_c \
 *      -framework Security -framework CoreFoundation -framework SystemConfiguration
 *
 * Run (macOS):
 *   DYLD_LIBRARY_PATH=target/debug ./icechunk-c/tests/smoke_test
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../include/icechunk.h"

int main(void) {
    int rc;

    printf("=== Icechunk C API Smoke Test ===\n");

    /* ------------------------------------------------------------ */
    /* 1. Create in-memory storage                                  */
    /* ------------------------------------------------------------ */
    printf("Creating in-memory storage... ");
    struct IcechunkStorage *storage = icechunk_storage_new_in_memory();
    assert(storage != NULL);
    printf("OK\n");

    /* ------------------------------------------------------------ */
    /* 2. Open a store on the "main" branch                         */
    /* ------------------------------------------------------------ */
    printf("Opening store on branch 'main'... ");
    struct IcechunkStore *store = icechunk_store_open(storage, "main");
    assert(store != NULL);
    printf("OK\n");

    /* ------------------------------------------------------------ */
    /* 3. Verify the store is writable (not read-only)              */
    /* ------------------------------------------------------------ */
    printf("Checking store is writable... ");
    int32_t ro = icechunk_store_is_read_only(store);
    assert(ro == 0);
    printf("OK\n");

    /* ------------------------------------------------------------ */
    /* 4. Set zarr.json metadata for an array                       */
    /* ------------------------------------------------------------ */
    printf("Setting array metadata... ");
    const char *meta_key = "myarray/zarr.json";
    const char *meta =
        "{\"zarr_format\":3,\"node_type\":\"array\",\"shape\":[4],"
        "\"data_type\":\"float32\","
        "\"chunk_grid\":{\"name\":\"regular\","
        "\"configuration\":{\"chunk_shape\":[4]}},"
        "\"chunk_key_encoding\":{\"name\":\"default\","
        "\"configuration\":{\"separator\":\"/\"}},"
        "\"fill_value\":0.0,"
        "\"codecs\":[{\"name\":\"bytes\","
        "\"configuration\":{\"endian\":\"little\"}}]}";
    rc = icechunk_store_set(store, meta_key, (const uint8_t *)meta, strlen(meta));
    assert(rc == ICECHUNK_SUCCESS);
    printf("OK\n");

    /* ------------------------------------------------------------ */
    /* 5. Set a chunk                                               */
    /* ------------------------------------------------------------ */
    printf("Setting chunk data... ");
    const char *chunk_key = "myarray/c/0";
    /* 16 bytes = 4 x float32 little-endian: [1.0, 2.0, 3.0, 4.0] */
    uint8_t chunk_data[] = {
        0x00, 0x00, 0x80, 0x3F,   /* 1.0f */
        0x00, 0x00, 0x00, 0x40,   /* 2.0f */
        0x00, 0x00, 0x40, 0x40,   /* 3.0f */
        0x00, 0x00, 0x80, 0x40    /* 4.0f */
    };
    rc = icechunk_store_set(store, chunk_key,
                            chunk_data, sizeof(chunk_data));
    assert(rc == ICECHUNK_SUCCESS);
    printf("OK\n");

    /* ------------------------------------------------------------ */
    /* 6. Verify the chunk key exists                               */
    /* ------------------------------------------------------------ */
    printf("Checking chunk exists... ");
    int32_t exists = icechunk_store_exists(store, chunk_key);
    assert(exists == 1);
    printf("OK\n");

    /* ------------------------------------------------------------ */
    /* 7. Get the chunk back and verify round-trip                  */
    /* ------------------------------------------------------------ */
    printf("Getting chunk data and verifying round-trip... ");
    uint8_t *out_data = NULL;
    size_t out_len = 0;
    rc = icechunk_store_get(store, chunk_key, &out_data, &out_len);
    assert(rc == ICECHUNK_SUCCESS);
    assert(out_data != NULL);
    assert(out_len == sizeof(chunk_data));
    assert(memcmp(out_data, chunk_data, out_len) == 0);
    free(out_data);
    out_data = NULL;
    printf("OK\n");

    /* ------------------------------------------------------------ */
    /* 8. Also verify metadata round-trip                           */
    /* ------------------------------------------------------------ */
    printf("Getting metadata and verifying round-trip... ");
    uint8_t *meta_out = NULL;
    size_t meta_out_len = 0;
    rc = icechunk_store_get(store, meta_key, &meta_out, &meta_out_len);
    assert(rc == ICECHUNK_SUCCESS);
    assert(meta_out != NULL);
    assert(meta_out_len == strlen(meta));
    assert(memcmp(meta_out, meta, meta_out_len) == 0);
    free(meta_out);
    meta_out = NULL;
    printf("OK\n");

    /* ------------------------------------------------------------ */
    /* 9. List all keys and verify we see at least 2                */
    /* ------------------------------------------------------------ */
    printf("Listing all keys...\n");
    struct IcechunkStoreListIter *iter = icechunk_store_list(store);
    assert(iter != NULL);
    int count = 0;
    while (1) {
        char *key = NULL;
        rc = icechunk_store_list_next(iter, &key);
        assert(rc == ICECHUNK_SUCCESS);
        if (key == NULL) break;
        printf("  listed key: %s\n", key);
        count++;
        free(key);
    }
    printf("  total keys listed: %d\n", count);
    assert(count >= 2);  /* at least zarr.json + chunk */
    icechunk_store_list_free(iter);
    iter = NULL;
    printf("List OK\n");

    /* ------------------------------------------------------------ */
    /* 10. Delete the chunk key                                     */
    /* ------------------------------------------------------------ */
    printf("Deleting chunk key... ");
    rc = icechunk_store_delete(store, chunk_key);
    assert(rc == ICECHUNK_SUCCESS);
    printf("OK\n");

    /* ------------------------------------------------------------ */
    /* 11. Verify it no longer exists                               */
    /* ------------------------------------------------------------ */
    printf("Verifying chunk is gone... ");
    exists = icechunk_store_exists(store, chunk_key);
    assert(exists == 0);
    printf("OK\n");

    /* ------------------------------------------------------------ */
    /* 12. Verify get on deleted key returns not-found              */
    /* ------------------------------------------------------------ */
    printf("Verifying get on deleted key returns error... ");
    uint8_t *del_data = NULL;
    size_t del_len = 0;
    rc = icechunk_store_get(store, chunk_key, &del_data, &del_len);
    assert(rc < 0);  /* ICECHUNK_ERROR_NOT_FOUND or ICECHUNK_ERROR */
    assert(del_data == NULL);
    printf("OK (rc=%d)\n", rc);

    /* ------------------------------------------------------------ */
    /* 13. Verify last error is set after failed get                */
    /* ------------------------------------------------------------ */
    printf("Checking last error message... ");
    const char *err = icechunk_last_error();
    assert(err != NULL);
    printf("OK (error: %s)\n", err);

    /* ------------------------------------------------------------ */
    /* 14. Cleanup                                                  */
    /* ------------------------------------------------------------ */
    printf("Freeing store... ");
    icechunk_store_free(store);
    printf("OK\n");

    printf("\nAll C smoke tests passed!\n");
    return 0;
}
