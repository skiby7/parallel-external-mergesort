
#ifndef _RECORD_HPP
#define _RECORD_HPP

#include <cstdint>
#include <cstring>
#include <memory>

typedef struct _Record {
    unsigned long key;
    uint32_t len;
    std::unique_ptr<char[]> rpayload;

    _Record() : key(0), len(0), rpayload(nullptr) {}

    _Record(unsigned long _key, uint32_t _len, const char *payload)
        : key(_key), len(_len), rpayload(std::make_unique<char[]>(_len)) {
        std::memcpy(rpayload.get(), payload, len);
    }

    _Record(const _Record& other)
        : key(other.key), len(other.len), rpayload(std::make_unique<char[]>(other.len)) {
        std::memcpy(rpayload.get(), other.rpayload.get(), len);
    }

    _Record(_Record&& other) noexcept
        : key(other.key), len(other.len), rpayload(std::move(other.rpayload)) {
        other.key = 0;
        other.len = 0;
    }

    _Record& operator=(const _Record& other) {
        if (this != &other) {
            key = other.key;
            len = other.len;
            rpayload = std::make_unique<char[]>(len);
            std::memcpy(rpayload.get(), other.rpayload.get(), len);
        }
        return *this;
    }

    _Record& operator=(_Record&& other) noexcept {
        if (this != &other) {
            key = other.key;
            len = other.len;
            rpayload = std::move(other.rpayload);
            other.key = 0;
            other.len = 0;
        }
        return *this;
    }

    ~_Record() = default;

    bool operator < (const _Record &a) const { return key < a.key; }
    bool operator <= (const _Record &a) const { return key <= a.key; }
    bool operator == (const _Record &a) const { return key == a.key; }
    bool operator >= (const _Record &a) const { return key >= a.key; }
    bool operator > (const _Record &a) const { return key > a.key; }

} Record;

struct RecordComparator {
    bool operator()(Record a, Record b){
        return (a.key > b.key);
    }
};

struct PairRecordComparator {
    bool operator()(const std::pair<Record, size_t>& a, const std::pair<Record, size_t>& b) const {
        return a.first > b.first;
    }
};

#endif // _RECORD_HPP
