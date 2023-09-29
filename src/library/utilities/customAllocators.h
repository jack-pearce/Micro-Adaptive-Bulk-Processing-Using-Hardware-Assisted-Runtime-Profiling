#ifndef MABPL_CUSTOMALLOCATORS_H
#define MABPL_CUSTOMALLOCATORS_H

#include <cstdlib>
#include <limits>
#include <memory>
#include <vector>
#include <optional>

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

        std::cout << "Custom allocator called" << std::endl; //////////////////////////////////////////////

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
        std::cout << "Constructor called" << std::endl; /////////////////////////////////////////////////
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

/*template <typename T>
class LazilyInitialisedVector {
public:
    LazilyInitialisedVector(std::size_t initialSize)
            : data(initialSize) {}

    T& operator[](std::size_t index) {
        if (!data[index].has_value()) {
            data[index] = T();
        }
        return *data[index];
    }

private:
    std::vector<std::optional<T>, CustomAllocator<std::optional<T>>> data;
};*/

template <typename T>
bool memoryIsAllZero(const T& object) {
    const char* begin = reinterpret_cast<const char*>(&object);
    const char* end = begin + sizeof(T);

    for (const char* ptr = begin; ptr < end; ++ptr) {
        if (*ptr != 0) {
            return false;
        }
    }

    return true;
}

template <typename T>
class LazyInitializationVector {
public:
    using value_type = T;
    using size_type = std::size_t;
    using allocator_type = std::allocator<T>;

    LazyInitializationVector(size_type size) : size_(size), data_(nullptr), elementCount_(0) {
        data_ = static_cast<pointer>(std::calloc(size_, sizeof(value_type)));
        if (!data_) {
            throw std::bad_alloc();
        }
    }

    ~LazyInitializationVector() {
        // Should add a way to destroy initialized objects
//        for (size_type i = 0; i < initialized_; ++i) {
//            data_[i].~value_type();
//        }
        std::free(data_);
    }

    value_type& operator[](size_type index) {
        if (index >= size_) {
            throw std::out_of_range("Index out of range.");
        }

        return getElement(index);
    }

    bool empty() const {
        return elementCount_ == 0;
    }

    value_type& back() {
        return getElement(size_ - 1);
    }

    value_type* data() {
        return data_;
    }

    allocator_type get_allocator() const {
        return allocator_type();
    }

    size_type size() const {
        return elementCount_;
    }

    size_type max_size() const {
        return std::numeric_limits<int>::max();
    }


    // range loop - shouldn't be required

    // clear - no change to memory, delete all elements - remove code in new hash map so this isn't required

    // copy constructor (with and without std::move) - remove code in new hash map so this isn't required

    // resize - remove code in new hash map so this isn't required

private:
    using pointer = typename std::allocator_traits<std::allocator<T>>::pointer;

    value_type& getElement(size_type index) {
        if (memoryIsAllZero(data_[index])) {
            std::cout << "Object constructed at index " << index << std::endl;
            new (&data_[index]) value_type();
        }

        return data_[index];
    }

    size_type size_;
    pointer data_;
    size_type elementCount_;
};



}

#endif //MABPL_CUSTOMALLOCATORS_H
