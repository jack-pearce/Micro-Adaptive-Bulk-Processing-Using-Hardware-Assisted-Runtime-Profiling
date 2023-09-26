#ifndef MABPL_CUSTOMALLOCATORS_H
#define MABPL_CUSTOMALLOCATORS_H

#include <cstdlib>
#include <limits>
#include <memory>

namespace MABPL {

template <typename T>
class CustomAllocator {
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    template <typename U>
    struct rebind {
        using other = CustomAllocator<U>;
    };

    CustomAllocator() noexcept = default;

    template <typename U>
    CustomAllocator(const CustomAllocator<U>&) noexcept {}

    pointer address(reference x) const noexcept { return &x; }

    const_pointer address(const_reference x) const noexcept { return &x; }

    pointer allocate(size_type n, const void* = 0) {
        if (n > max_size()) {
            throw std::bad_alloc();
        }

        std::cout << "Custom allocator called" << std::endl;

        pointer ptr = static_cast<pointer>(std::calloc(n, sizeof(value_type)));
//        pointer ptr = static_cast<pointer>(std::malloc(n * sizeof(value_type)));
        if (!ptr) {
            throw std::bad_alloc();
        }
        return ptr;
    }

    void deallocate(pointer p, size_type n) noexcept {
        if (p) {
            std::free(p);
        }
    }

    size_type max_size() const noexcept {
        return std::numeric_limits<size_type>::max() / sizeof(value_type);
    }

    void construct(pointer p, const_reference value) {
        new (static_cast<void*>(p)) T(value);
    }

    void destroy(pointer p) noexcept {
        if (p) {
            p->~T();
        }
    }
};

template <typename T, typename U>
bool operator==(const CustomAllocator<T>&, const CustomAllocator<U>&) noexcept {
    return true;
}

template <typename T, typename U>
bool operator!=(const CustomAllocator<T>&, const CustomAllocator<U>&) noexcept {
    return false;
}

}

#endif //MABPL_CUSTOMALLOCATORS_H
