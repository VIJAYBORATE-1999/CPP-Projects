#include <iostream>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/crc.hpp> // CRC header
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <sstream>
#include "UserMessages_Data.h"
#include "../../common/include/logger.h"
#include "../../common/include/config.h"
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <cstdlib>
#include <ctime>
#include <chrono>
#include <thread>
#include <vector>

// Define a namespace alias for ease of use
namespace bip = boost::interprocess;

// Function to generate a random 10-digit mobile number
long long int generate_random_mobile_number() {
    return 1000000000LL + rand() % 9000000000LL;
}

// Function to generate a random IP address
std::string generate_random_ip_address() {
    return std::to_string(rand() % 256) + "." +
           std::to_string(rand() % 256) + "." +
           std::to_string(rand() % 256) + "." +
           std::to_string(rand() % 256);
}

// Function to generate a random creation time in HHMMSSMilliseconds format
int generate_random_creation_time() {
    time_t now = time(0);
    tm *ltm = localtime(&now);
    int milliseconds = rand() % 1000;
    return (ltm->tm_hour * 10000000) + (ltm->tm_min * 100000) + (ltm->tm_sec * 1000) + milliseconds;
}

// Function to generate a random name
std::string generate_random_name() {
    const std::vector<std::string> names = {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Davis"};
    return names[rand() % names.size()];
}

// Function to generate a random email
std::string generate_random_email(const std::string& name) {
    std::string email = name;
    std::replace(email.begin(), email.end(), ' ', '.');
    email += "@example.com";
    return email;
}

// Function to compute CRC checksum
std::uint32_t compute_crc(const UserMessages_Data& msg) {
    boost::crc_32_type crc;
    crc.process_bytes(&msg.transaction_type, sizeof(msg.transaction_type));
    crc.process_bytes(&msg.creation_time, sizeof(msg.creation_time));
    crc.process_bytes(&msg.mobile_number, sizeof(msg.mobile_number));
    crc.process_bytes(msg.ip_address.c_str(), msg.ip_address.size());

    for (const auto& entry : msg.personal_identifier_info) {
        crc.process_bytes(entry.first.c_str(), entry.first.size());
        crc.process_bytes(entry.second.c_str(), entry.second.size());
    }

    return crc.checksum();
}

void log_transaction_times(const std::vector<std::chrono::microseconds>& times) {
    auto total_time = std::chrono::microseconds::zero();
    for (const auto& time : times) {
        total_time += time;
    }
    auto avg_time = total_time / times.size();

    std::cout << "Total time for all transactions: " << total_time.count() << " microseconds" << std::endl;
    std::cout << "Average time per transaction: " << avg_time.count() << " microseconds" << std::endl;

    spdlog::info("Total time for all transactions: {} microseconds", total_time.count());
    spdlog::info("Average time per transaction: {} microseconds", avg_time.count());
}

int main() {
    loadConfig();
    LOGGER_LEVEL_SETTT = getConfigValue("LOGGER_LEVEL_SETTT");
    int DATA_BULK_COUNTT = std::stoi(getConfigValue("DATA_BULK_COUNT"));
    NAME_SHARED_MEMORY = getConfigValue("NAME_SHARED_MEMORY");
    const char* name_of_shared_memeory = NAME_SHARED_MEMORY.c_str();
    std::string size_memeory = getConfigValue("SHARED_MEMORY_SIZE");
    std::size_t size_of_shared_memeory = static_cast<std::size_t>(std::stoull(size_memeory));

    srand(time(0)); // Seed the random number generator

    setup_logger("sender_logger", "sender_log.txt", LOGGER_LEVEL_SETTT);

    try {
        // Open or create shared memory
        bip::managed_shared_memory segment(bip::open_or_create, name_of_shared_memeory, size_of_shared_memeory);

        // Define shared memory allocator and vector
        typedef bip::allocator<char, bip::managed_shared_memory::segment_manager> CharAllocator;
        typedef bip::basic_string<char, std::char_traits<char>, CharAllocator> ShmemString;
        typedef bip::allocator<ShmemString, bip::managed_shared_memory::segment_manager> ShmemStringAllocator;
        typedef bip::vector<ShmemString, ShmemStringAllocator> ShmemVector;

        CharAllocator char_alloc(segment.get_segment_manager());
        ShmemStringAllocator shmem_string_alloc(segment.get_segment_manager());
        ShmemVector* messages = segment.find_or_construct<ShmemVector>("Messages")(shmem_string_alloc);

        std::vector<std::chrono::microseconds> transaction_times;

        // Fill the vector with random data
        for (int i = 1; i <= DATA_BULK_COUNTT; ++i) {
            auto start_time = std::chrono::high_resolution_clock::now();

            UserMessages_Data msg(char_alloc);
            msg.transaction_type = i;
            msg.creation_time = generate_random_creation_time();
            msg.mobile_number = generate_random_mobile_number();
            msg.ip_address = ShmemString(generate_random_ip_address().c_str(), char_alloc);

            // Generate random personal info
            std::string name = generate_random_name();
            std::string email = generate_random_email(name);
            msg.personal_identifier_info.insert(std::make_pair(ShmemString("Name", char_alloc), ShmemString(name.c_str(), char_alloc)));
            msg.personal_identifier_info.insert(std::make_pair(ShmemString("Email", char_alloc), ShmemString(email.c_str(), char_alloc)));

            // Compute CRC checksum
            std::uint32_t checksum = compute_crc(msg);
            msg.checksum = checksum;

            // Set filter_id to an empty string initially
            msg.filter_id = ShmemString("", char_alloc);

            // Serialize the message
            std::ostringstream oss;
            boost::archive::text_oarchive oa(oss);
            oa << msg;
            std::string serialized_msg = oss.str();

            // Print the serialized message
            std::cout << "Serialized Message: " << serialized_msg << std::endl;

            // Store the serialized message in shared memory
            messages->push_back(ShmemString(serialized_msg.c_str(), char_alloc));

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            transaction_times.push_back(duration);

            std::cout << "Transaction Type: " << msg.transaction_type
                      << ", Creation Time: " << msg.creation_time
                      << ", Mobile Number: " << msg.mobile_number
                      << ", IP Address: " << msg.ip_address
                      << ", Personal Info: {Name: " << name << ", Email: " << email << "}"
                      << ", Checksum: " << checksum
                      << ", Time Taken: " << duration.count() << " μs" << std::endl << std::endl << std::endl;

            spdlog::info("Transaction Type: {}, Creation Time: {}, Mobile Number: {}, IP Address: {}, Personal Info: {{Name: {}, Email: {}}}, Checksum: {}, Time Taken: {} μs",
                         msg.transaction_type, msg.creation_time, msg.mobile_number, msg.ip_address.c_str(), name, email, checksum, duration.count());
        }

        std::cout << "All Messages written to shared memory Except Time.." << std::endl;
        spdlog::info("All Messages written to shared memory Except Time..");

        // Create a thread to log transaction times
        std::thread log_thread(log_transaction_times, std::ref(transaction_times));
        log_thread.join();

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        spdlog::error("Error: {}", e.what());
    }

    return 0;
}