#include <iostream>
#include <unistd.h>
#include <thread>
#include <unistd.h>

#include "systemInformation.h"

namespace MABPL {

long l3cacheSize() {
    return sysconf(_SC_LEVEL3_CACHE_SIZE);
}

long bytesPerCacheLine() {
    return sysconf(_SC_LEVEL1_DCACHE_LINESIZE);
}

int maxDop() {
    return static_cast<int>(std::thread::hardware_concurrency());
}

std::string getCurrentWorkingDirectory() {
    char buffer[FILENAME_MAX];
    if (getcwd(buffer, sizeof(buffer)) != nullptr) {
        return {buffer};
    } else {
        std::cerr << "Error getting current working directory" << std::endl;
        exit(1);
    }
}

}
