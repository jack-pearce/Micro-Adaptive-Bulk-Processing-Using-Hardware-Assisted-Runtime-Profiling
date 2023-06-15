#include <unistd.h>

#include "hardwareTuning.h"

long getL1cacheLineSize() {
    return sysconf(_SC_LEVEL1_DCACHE_LINESIZE);
}
