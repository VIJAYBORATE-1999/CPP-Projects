#ifndef USER_MESSAGES_DATA_H
#define USER_MESSAGES_DATA_H

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/nvp.hpp>
#include <iostream>
#include <cstdint> // Include this header for std::uint32_t

// Define a namespace alias for ease of use
namespace bip = boost::interprocess;

// Define the shared memory allocator
typedef bip::allocator<char, bip::managed_shared_memory::segment_manager> ShmemCharAllocator;
typedef bip::basic_string<char, std::char_traits<char>, ShmemCharAllocator> ShmemString;

// Define ShmemMap to represent a map in shared memory
typedef bip::allocator<std::pair<const ShmemString, ShmemString>, bip::managed_shared_memory::segment_manager> ShmemMapAllocator;
typedef bip::map<ShmemString, ShmemString, std::less<ShmemString>, ShmemMapAllocator> ShmemMap;

// Custom serialization for ShmemString
namespace boost {
namespace serialization {

template<class Archive>
void save(Archive& ar, const ShmemString& str, const unsigned int version) {
    std::string std_str(str.c_str());
    ar & boost::serialization::make_nvp("std_str", std_str);
}

template<class Archive>
void load(Archive& ar, ShmemString& str, const unsigned int version) {
    std::string std_str;
    ar & boost::serialization::make_nvp("std_str", std_str);
    str = ShmemString(std_str.c_str(), str.get_allocator());
}

template<class Archive>
void serialize(Archive& ar, ShmemString& str, const unsigned int version) {
    boost::serialization::split_free(ar, str, version);
}

} // namespace serialization
} // namespace boost

// Custom serialization for ShmemMap
namespace boost {
namespace serialization {

template<class Archive>
void save(Archive& ar, const ShmemMap& map, const unsigned int version) {
    std::map<std::string, std::string> std_map;
    for (const auto& item : map) {
        std_map[item.first.c_str()] = item.second.c_str();
    }
    ar & boost::serialization::make_nvp("std_map", std_map);
}

template<class Archive>
void load(Archive& ar, ShmemMap& map, const unsigned int version) {
    std::map<std::string, std::string> std_map;
    ar & boost::serialization::make_nvp("std_map", std_map);
    for (const auto& item : std_map) {
        map.insert(std::make_pair(ShmemString(item.first.c_str(), map.get_allocator()), ShmemString(item.second.c_str(), map.get_allocator())));
    }
}

template<class Archive>
void serialize(Archive& ar, ShmemMap& map, const unsigned int version) {
    boost::serialization::split_free(ar, map, version);
}

} // namespace serialization
} // namespace boost

struct UserMessages_Data {
    int transaction_type;
    int creation_time;
    long long int mobile_number;
    ShmemString ip_address;
    ShmemMap personal_identifier_info;
    std::uint32_t checksum; // Add this field
    ShmemString filter_id; // Change this field to ShmemString

    // Constructor for shared memory usage
    UserMessages_Data(const ShmemCharAllocator& alloc)
        : ip_address(alloc), personal_identifier_info(alloc), filter_id(alloc) {}

    // Serialization function
    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & boost::serialization::make_nvp("transaction_type", transaction_type);
        ar & boost::serialization::make_nvp("creation_time", creation_time);
        ar & boost::serialization::make_nvp("mobile_number", mobile_number);
        ar & boost::serialization::make_nvp("ip_address", ip_address);
        ar & boost::serialization::make_nvp("personal_identifier_info", personal_identifier_info);
        ar & boost::serialization::make_nvp("checksum", checksum); // Serialize checksum
        ar & boost::serialization::make_nvp("filter_id", filter_id); // Serialize filter_id
    }
};

// Custom operator<< for ShmemString
inline std::ostream& operator<<(std::ostream& os, const ShmemString& str) {
    os << str.c_str();
    return os;
}

// Custom operator<< for std::pair containing ShmemString
inline std::ostream& operator<<(std::ostream& os, const std::pair<const ShmemString, ShmemString>& p) {
    os << p.first << ": " << p.second;
    return os;
}

#endif // USER_MESSAGES_DATA_H