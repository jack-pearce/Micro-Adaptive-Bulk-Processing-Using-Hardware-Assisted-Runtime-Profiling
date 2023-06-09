#include <iostream>

#include "libs/papi/papi-7.0.1/src/install/include/papi.h"

int main()
{
    int retval, EventSet = PAPI_NULL;
    unsigned int native = 0x0;
    PAPI_event_info_t info;

    /* Initialize the library */
    retval = PAPI_library_init(PAPI_VER_CURRENT);
    if (retval != PAPI_VER_CURRENT) {
        printf("PAPI library init error!\n");
        exit(1);
    }

    /* Create an EventSet */
    retval = PAPI_create_eventset(&EventSet);
    if (retval != PAPI_OK) {
        std::cout << "Return value: " << retval << std::endl;
    }

    /* Find the first available native event */
    native = PAPI_NATIVE_MASK | 0;
    retval = PAPI_enum_event(reinterpret_cast<int *>(&native), PAPI_ENUM_FIRST);
    if (retval != PAPI_OK) {
        std::cout << "Return value: " << retval << std::endl;
    }

    /* Add it to the eventset */
    retval = PAPI_add_event(EventSet, native);
    if (retval != PAPI_OK) {
        std::cout << "Return value: " << retval << std::endl;
    }

    /* Exit successfully */
    printf("Success!\n");
    exit(0);
}