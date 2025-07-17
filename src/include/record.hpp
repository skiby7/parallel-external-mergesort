
#ifndef _RECORD_HPP
#define _RECORD_HPP
#include <cstdint>
#include <cstring>
#include <memory>

typedef struct _Record {
    unsigned long key;
    uint32_t len;
    char* payload() {
        return reinterpret_cast<char*>(this + 1); // points to memory immediately after Record
    }

    const char* payload() const {
        return reinterpret_cast<const char*>(this + 1);
    }


    bool operator < (const _Record &a) const { return key < a.key; }
    bool operator <= (const _Record &a) const { return key <= a.key; }
    bool operator == (const _Record &a) const { return key == a.key; }
    bool operator >= (const _Record &a) const { return key >= a.key; }
    bool operator > (const _Record &a) const { return key > a.key; }

} Record;

#endif // _RECORD_HPP
