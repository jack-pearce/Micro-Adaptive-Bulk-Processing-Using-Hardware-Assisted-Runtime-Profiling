#ifndef MABPL_LAZY_INITIALISATION_ARRAY_H
#define MABPL_LAZY_INITIALISATION_ARRAY_H

#include <cstdlib>
#include <limits>
#include <memory>

template <typename T>
class LazyInitializationArray {
public:
    using value_type = T;
    using size_type = std::size_t;
    using allocator_type = std::allocator<T>;

    LazyInitializationArray(size_type size) : size_(size), data_(nullptr), elementCount_(0) {
        data_ = static_cast<pointer>(std::calloc(size_, sizeof(value_type)));
        if (!data_) {
            throw std::bad_alloc();
        }
    }

    ~LazyInitializationArray() {
        // Note that individual elements are not free'd since we do not track their location
        std::free(data_);
    }

    value_type& operator[](size_type index) {
//        if (index >= size_) {
//            throw std::out_of_range("Index out of range.");
//        }
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

    const value_type* data() const {
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

private:
    using pointer = typename std::allocator_traits<std::allocator<T>>::pointer;

    inline value_type& getElement(size_type index) {
        if (!(*reinterpret_cast<bool*>(data_ + index))) {
            new (&data_[index]) value_type();
        }
        return data_[index];
    }

    size_type size_;
    pointer data_;
    size_type elementCount_;
};


#endif //MABPL_LAZY_INITIALISATION_ARRAY_H
