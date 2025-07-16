#ifndef ARENA_HPP
#define ARENA_HPP

#include <memory>
#include <vector>
#include <queue>
#include <deque>
#include <mutex>
#include <cstddef>
#include <stdexcept>
#include <iostream>

class MemoryArena {
private:
    std::unique_ptr<char[]> buffer;
    size_t size;
    size_t offset;
    mutable std::mutex mutex;

    // Helper function for alignment
    static size_t align_up(size_t value, size_t alignment) {
        return (value + alignment - 1) & ~(alignment - 1);
    }

public:
    explicit MemoryArena(size_t arena_size)
        : buffer(std::make_unique<char[]>(arena_size)),
          size(arena_size),
          offset(0) {
        std::cout << "Arena created with " << arena_size << " bytes\n";
    }

    void* allocate(size_t bytes, size_t alignment = alignof(std::max_align_t)) {
        std::lock_guard<std::mutex> lock(mutex);

        if (bytes == 0) return nullptr;

        // Align the offset
        size_t aligned_offset = align_up(offset, alignment);

        if (aligned_offset + bytes > size) {
            std::cerr << "Arena allocation failed: requested " << bytes
                      << " bytes, available " << (size - aligned_offset)
                      << " bytes\n";
            throw std::bad_alloc();
        }

        void* ptr = buffer.get() + aligned_offset;
        offset = aligned_offset + bytes;


        return ptr;
    }

    void reset() {
        std::lock_guard<std::mutex> lock(mutex);
        offset = 0;
        std::cout << "Arena reset\n";
    }

    size_t bytes_used() const {
        std::lock_guard<std::mutex> lock(mutex);
        return offset;
    }

    size_t bytes_available() const {
        std::lock_guard<std::mutex> lock(mutex);
        return size - offset;
    }

    void print_stats() const {
        std::lock_guard<std::mutex> lock(mutex);
        std::cout << "Arena stats: " << offset << "/" << size
                  << " bytes used (" << (100.0 * offset / size) << "%)\n";
    }
};

template<typename T>
class ArenaAllocator {
private:
    MemoryArena* arena;

public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    template<typename U>
    struct rebind {
        using other = ArenaAllocator<U>;
    };

    // Constructors
    ArenaAllocator() : arena(nullptr) {}
    explicit ArenaAllocator(MemoryArena* a) : arena(a) {}

    template<typename U>
    ArenaAllocator(const ArenaAllocator<U>& other) : arena(other.get_arena()) {}

    pointer allocate(size_type n) {
        if (!arena) {
            // Fall back to standard allocator if no arena
            return static_cast<pointer>(::operator new(n * sizeof(T)));
        }

        if (n == 0) return nullptr;

        // Check for overflow
        if (n > std::numeric_limits<size_type>::max() / sizeof(T)) {
            throw std::bad_alloc();
        }

        return static_cast<pointer>(arena->allocate(n * sizeof(T), alignof(T)));
    }

    void deallocate(pointer ptr, size_type n) {
        if (!arena) {
            // If we used standard allocator, free it
            ::operator delete(ptr);
        }
        // Arena doesn't support individual deallocation
    }

    template<typename U>
    bool operator==(const ArenaAllocator<U>& other) const {
        return arena == other.get_arena();
    }

    template<typename U>
    bool operator!=(const ArenaAllocator<U>& other) const {
        return !(*this == other);
    }

    // Helper to access arena from other template instantiations
    MemoryArena* get_arena() const { return arena; }

    template<typename U>
    friend class ArenaAllocator;
};

// Convenience type aliases
template<typename T>
using ArenaVector = std::vector<T, ArenaAllocator<T>>;

template<typename T, typename Compare = std::less<T>>
using ArenaPriorityQueue = std::priority_queue<T, ArenaVector<T>, Compare>;

template<typename T>
using ArenaDeque = std::deque<T, ArenaAllocator<T>>;

#endif // ARENA_HPP
