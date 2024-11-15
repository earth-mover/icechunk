#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct Repository Repository;

typedef uint16_t IcechunkFormatVersion;

#define LATEST_ICECHUNK_MANIFEST_FORMAT 0

#define LATEST_ICECHUNK_SNAPSHOT_FORMAT 0

struct Repository *create_inmemory_repository(void);

struct Repository *icechunk_add_root_group(struct Repository *dsboxed);
