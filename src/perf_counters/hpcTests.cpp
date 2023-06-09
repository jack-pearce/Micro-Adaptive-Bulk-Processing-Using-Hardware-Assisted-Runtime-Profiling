#include <iostream>

#include "../../libs/papi/src/install/include/papi.h"


void hpcTest_1() {}

void hpcTest_2() {
        long_long start_cycles, end_cycles, start_usec, end_usec;
        int EventSet = PAPI_NULL;

        if (PAPI_library_init(PAPI_VER_CURRENT) != PAPI_VER_CURRENT)
        exit(1);

/* Gets the starting time in clock cycles */
        start_cycles = PAPI_get_real_cyc();

/* Gets the starting time in microseconds */
        start_usec = PAPI_get_real_usec();

/*Create an EventSet */
        if (PAPI_create_eventset(&EventSet) != PAPI_OK)
        exit(1);
/* Gets the ending time in clock cycles */
        end_cycles = PAPI_get_real_cyc();

/* Gets the ending time in microseconds */
        end_usec = PAPI_get_real_usec();

        printf("Wall clock cycles: %lld\n", end_cycles - start_cycles);
        printf("Wall clock time in microseconds: %lld\n", end_usec - start_usec);
}

void hpcTest_3() {
    int retval, EventSet=PAPI_NULL;
    long_long values[1];

/* Initialize the PAPI library */
    retval = PAPI_library_init(PAPI_VER_CURRENT);
    if (retval != PAPI_VER_CURRENT) {
        fprintf(stderr, "PAPI library init error!\n");
        exit(1);
    }

/* Get event code */
    int EventCode;
    retval = PAPI_event_name_to_code("INSTRUCTION_RETIRED",&EventCode);
    if (retval != PAPI_OK) {
        fprintf(stderr, "PAPI could not create event code!\n");
        exit(1);
    }

/* Create the Event Set */
    if (PAPI_create_eventset(&EventSet) != PAPI_OK) {
        fprintf(stderr, "PAPI could not create eventset!\n");
        exit(1);
    }

/* Add Events to our Event Set */
    retval = PAPI_add_event(EventSet, EventCode);
    if (retval != PAPI_OK) {
        fprintf(stderr, "Could not add total instructions to event set!\n");
        std::cout << "Error code: " << retval << std::endl;
        exit(1);
    }

/* Start counting events in the Event Set */
    if (PAPI_start(EventSet) != PAPI_OK)
        exit(1);


/* Read the counting events in the Event Set */
//    if (PAPI_read(EventSet, values) != PAPI_OK)
//        exit(1);
//
//    printf("After reading the counters: %lld\n",values[0]);

/* Reset the counting events in the Event Set */
//    if (PAPI_reset(EventSet) != PAPI_OK)
//        exit(1);

    int sum = 0;
    for (int i = 0; i < 10000; i++) {
        sum += i;
    }

/* Add the counters in the Event Set */
//    if (PAPI_accum(EventSet, values) != PAPI_OK)
//        exit(1);
//    printf("After adding the counters: %lld\n",values[0]);

/* Stop the counting of events in the Event Set */
    if (PAPI_stop(EventSet, values) != PAPI_OK)
        exit(1);

    printf("After stopping the counters: %lld\n",values[0]);
}