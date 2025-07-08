
#ifndef _RECORD_HPP
#define _RECORD_HPP
#include <cstdint>
#include <cstring>

typedef struct _Record {
    unsigned long key;
    uint32_t len;
    char *rpayload;

    _Record() : key(0), len(0), rpayload(nullptr) {}

    _Record(unsigned long _key, uint32_t _len, char *payload) {
        key = _key;
        len = _len;
        rpayload = new char[len];
        memmove(rpayload, payload, len);
    }


    _Record(const _Record& other) {
        key = other.key;
        len = other.len;
        rpayload = new char[len];
        memmove(rpayload, other.rpayload, len);
    }


    _Record(_Record&& other) noexcept {
        key = other.key;
        len = other.len;
        rpayload = other.rpayload;


        other.rpayload = nullptr;
        other.len = 0;
        other.key = 0;
    }


    _Record& operator=(const _Record& other) {
        if (this != &other) {
            delete[] rpayload;
            key = other.key;
            len = other.len;
            rpayload = new char[len];
            memmove(rpayload, other.rpayload, len);
        }
        return *this;
    }


    _Record& operator=(_Record&& other) noexcept {
        if (this != &other) {
            delete[] rpayload;

            key = other.key;
            len = other.len;
            rpayload = other.rpayload;


            other.rpayload = nullptr;
            other.len = 0;
            other.key = 0;
        }
        return *this;
    }

    ~_Record() {
        delete[] rpayload;
    }

    bool operator < (const _Record &a) const {
        return key < a.key;
    }

    bool operator <= (const _Record &a) const {
        return key <= a.key;
    }

    bool operator == (const _Record &a) const {
        return key == a.key;
    }

    bool operator >= (const _Record &a) const {
        return key >= a.key;
    }

    bool operator > (const _Record &a) const {
        return key > a.key;
    }
} Record;

#endif // _RECORD_HPP
