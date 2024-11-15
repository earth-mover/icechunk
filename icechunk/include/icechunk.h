#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct Repository Repository;

typedef uint16_t IcechunkFormatVersion;

#define LATEST_ICECHUNK_MANIFEST_FORMAT 0

#define LATEST_ICECHUNK_SNAPSHOT_FORMAT 0

int create_inmemory_repository(struct Repository **ptr);

int icechunk_add_root_group(struct Repository *ptr);

int icechunk_add_group(struct Repository *ptr, const char *group_name_ptr);

void icechunk_free_repository(struct Repository *repo);
