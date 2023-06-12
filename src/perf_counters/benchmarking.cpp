#include <iostream>
#include <functional>

#include "benchmarking.h"
#include "../../libs/papi/src/install/include/papi.h"

/*// Decorator function that takes a function as an argument
template <typename Func>
auto hpcBenchmarkDecorator(Func&& func) {
    // Define the wrapper function
    return [=](auto&&... args) {
        // Perform pre-processing before calling the function
        std::cout << "Decorator: Before function call" << std::endl;

        // Call the original function
        auto result = func(std::forward<decltype(args)>(args)...);

        // Perform post-processing after calling the function
        std::cout << "Decorator: After function call" << std::endl;

        // Return the result
        return result;
    };
}*/