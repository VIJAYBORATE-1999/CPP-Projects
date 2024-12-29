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
#include <chrono>
#include <thread>
#include <vector>
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <rabbitmq-c/framing.h>
#include "../../lib/nlohmann_Json_Header/nlohmann/json.hpp" // Include the JSON library
#include <fstream>
#include <unordered_map>
#include <regex>

namespace bip = boost::interprocess;
using json = nlohmann::json; // Alias for the JSON library

// Compute CRC checksum
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

void log_read_times(const std::vector<std::chrono::microseconds>& times) {
    auto total_time = std::chrono::microseconds::zero();
    for (const auto& time : times) {
        total_time += time;
    }
    auto avg_time = total_time / times.size();

    std::cout << "Total time for reading all transactions: " << total_time.count() << " microseconds" << std::endl;
    std::cout << "Average time per transaction: " << avg_time.count() << " microseconds" << std::endl;

    spdlog::info("Total time for reading all transactions: {} microseconds", total_time.count());
    spdlog::info("Average time per transaction: {} microseconds", avg_time.count());
}

void send_to_rabbitmq(const std::vector<UserMessages_Data>& messages, const std::string& queue_name, const std::string& max_bundle_size_allowed) {
    // RabbitMQ connection setup
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    if (!socket) {
        std::cerr << "Creating TCP socket failed" << std::endl;
        spdlog::error("Creating TCP socket failed");
        return;
    }

    int status = amqp_socket_open(socket, "localhost", 5672);
    if (status) {
        std::cerr << "Opening TCP socket failed" << std::endl;
        spdlog::error("Opening TCP socket failed");
        return;
    }

    amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    amqp_channel_open(conn, 1);
    amqp_get_rpc_reply(conn);

    // Declare the queue
    amqp_queue_declare(conn, 1, amqp_cstring_bytes(queue_name.c_str()), 0, 0, 0, 1, amqp_empty_table);
    amqp_get_rpc_reply(conn);

    std::string bundle_body = "Transaction Type,Creation Time,Mobile Number,IP Address,Personal Info,FilterID\n";
    size_t bundle_size = bundle_body.size();
    const size_t max_bundle_size = static_cast<std::size_t>(std::stoull(max_bundle_size_allowed)); // 1 KB

    for (const auto& msg : messages) {
        json personal_info_json;
        for (const auto& entry : msg.personal_identifier_info) {
            personal_info_json[entry.first.c_str()] = entry.second.c_str();
        }
        std::string personal_info_str = personal_info_json.dump();

        // Remove the comma between Email and Name
        size_t pos = personal_info_str.find("\",\"");
        if (pos != std::string::npos) {
            personal_info_str.replace(pos, 3, "\" \"");
        }

        std::string message_body = std::to_string(msg.transaction_type) + "," +
                                   std::to_string(msg.creation_time) + "," +
                                   std::to_string(msg.mobile_number) + "," +
                                   msg.ip_address.c_str() + "," +
                                   personal_info_str + "," +
                                   msg.filter_id.c_str() + "\n";

        bundle_body += message_body;
        bundle_size += message_body.size();

        // Print the bundle size after each transaction
        std::cout << message_body << "Bundle Size: " << bundle_size << " bytes" << std::endl;
        spdlog::info("{}Bundle Size: {} bytes", message_body, bundle_size);

        if (bundle_size >= max_bundle_size) {
            amqp_bytes_t message_bytes;
            message_bytes.len = bundle_body.size();
            message_bytes.bytes = (void*)bundle_body.c_str();

            // Log and print the bundle size before sending
            std::cout << "Sending bundle to RabbitMQ, size: " << bundle_size << " bytes" << std::endl;
            spdlog::info("Sending bundle to RabbitMQ, size: {} bytes", bundle_size);

            int publish_status = amqp_basic_publish(conn, 1, amqp_cstring_bytes(""), amqp_cstring_bytes(queue_name.c_str()),
                                                    0, 0, NULL, message_bytes);
            if (publish_status != AMQP_STATUS_OK) {
                std::cerr << "Failed to publish bundle" << std::endl;
                spdlog::error("Failed to publish bundle");
            } else {
                std::cout << "Bundle published successfully" << std::endl;
                spdlog::info("Bundle published successfully");
            }

            bundle_body = "Transaction Type,Creation Time,Mobile Number,IP Address,Personal Info,FilterID\n";
            bundle_size = bundle_body.size();
        }
    }

    if (bundle_size > strlen("Transaction Type,Creation Time,Mobile Number,IP Address,Personal Info,FilterID\n")) {
        amqp_bytes_t message_bytes;
        message_bytes.len = bundle_body.size();
        message_bytes.bytes = (void*)bundle_body.c_str();

        // Log and print the final bundle size before sending
        std::cout << "Sending final bundle to RabbitMQ, size: " << bundle_size << " bytes" << std::endl;
        spdlog::info("Sending final bundle to RabbitMQ, size: {} bytes", bundle_size);

        int publish_status = amqp_basic_publish(conn, 1, amqp_cstring_bytes(""), amqp_cstring_bytes(queue_name.c_str()),
                                                0, 0, NULL, message_bytes);
        if (publish_status != AMQP_STATUS_OK) {
            std::cerr << "Failed to publish final bundle" << std::endl;
            spdlog::error("Failed to publish final bundle");
        } else {
            std::cout << "Final bundle published successfully" << std::endl;
            spdlog::info("Final bundle published successfully");
        }
    }

    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
}

class QueryFilter {
public:
    struct Query {
        std::string id = "0";
        std::map<std::string, std::string> queries;
        std::string operation; // AND, OR, BOTH
    };

    std::vector<Query> queries_;

    QueryFilter(const std::string& query_file_path) {
        load_queries(query_file_path);
    }

    void load_queries(const std::string& query_file_path) {
        std::ifstream query_file(query_file_path);
        if (!query_file.is_open()) {
            throw std::runtime_error("Failed to open query file");
        }

        std::string line;
        Query current_query;

        while (std::getline(query_file, line)) {
            if (line.empty()) continue;

            if (line.find("# AND query") != std::string::npos) {
                if (current_query.id != "0") {
                    queries_.push_back(current_query);
                    current_query = Query();
                }
                current_query.operation = "AND";
                std::cout << "Found AND query\n";
            } else if (line.find("# OR query") != std::string::npos) {
                if (current_query.id != "0") {
                    queries_.push_back(current_query);
                    current_query = Query();
                }
                current_query.operation = "OR";
                std::cout << "Found OR query\n";
            } else if (line.find("# Both AND and OR query") != std::string::npos) {
                if (current_query.id != "0") {
                    queries_.push_back(current_query);
                    current_query = Query();
                }
                current_query.operation = "BOTH";
                std::cout << "Found BOTH query\n";
                 } else if (line.find("ID") != std::string::npos) {
                if (current_query.id != "0") {
                    queries_.push_back(current_query);
                    current_query = Query();
                }
                current_query.id = line.substr(line.find(" ") + 1);
                std::cout << "Found ID: " << current_query.id << "\n";
            } else {
                std::cout << std::endl << std::endl << std::endl;
                std::cout << "/////////The line is////////////////" << line << std::endl;
                std::cout << std::endl << std::endl << std::endl;
                auto pos = line.find(" ");

                if (current_query.operation == "OR") {
                    std::istringstream iss(line);
                    std::string segment;
                    std::vector<std::string> tokens;
                    // Split the string by commas
                    while (std::getline(iss, segment, ',')) {
                        std::istringstream iss(segment);
                        std::string key, val;

                        // Split each segment by spaces
                        iss >> key >> val;
                        tokens.push_back(key);
                        tokens.push_back(val);
                    }

                    // Process the tokens in pairs
                    for (size_t i = 0; i < tokens.size(); i += 2) {
                        std::string token_key = tokens[i];
                        std::string token_value = tokens[i + 1];
                        std::cout << "OR CASE => string token_key = " << token_key << " and string token_value = " << token_value << std::endl;
                        current_query.queries[token_key] = token_value;
                    }
                } else if (current_query.operation == "BOTH") {
                    std::istringstream iss(line);
                    std::string segment;
                    std::vector<std::string> tokens;
                    while (std::getline(iss, segment, ',')) {
                        std::istringstream segmentStream(segment);
                        std::string key, val;

                        // Split each segment by spaces
                        segmentStream >> key >> val;
                        tokens.push_back(key);
                        tokens.push_back(val);
                    }

                    // Process the tokens in pairs
                    for (size_t i = 0; i < tokens.size(); i += 2) {
                        std::string token_key = tokens[i];
                        std::string token_value = tokens[i + 1];
                        std::cout << "BOTH CASE=>  string token_key = " << token_key << " and string token_value = " << token_value << std::endl;
                        current_query.queries[token_key] = token_value;
                    }
                } else {
                    std::istringstream iss(line);
                    std::string key, val;

                    // Split the string by spaces
                    while (iss >> key >> val) {
                        std::cout << "string token_key = " << key << " and string token_value = " << val << std::endl;
                        current_query.queries[key] = val;
                    }
                }
            }
        }

        if (current_query.id != "0") {
            queries_.push_back(current_query);
        }

        query_file.close();
    }

    std::string match_query(UserMessages_Data& msg) const {
        std::string matched_ids;
        for (const auto& query : queries_) {
            bool match = false;
            std::cout << "Checking Query ID: " << query.id << " with operation: " << query.operation << "\n";

            if (query.operation == "AND") {
                match = true;
                for (const auto& condition : query.queries) {
                    std::cout << "Condition FIRST FOR AND: " << condition.first << std::endl;
                    std::cout << "Condition SECOND FOR AND: " << condition.second << std::endl;
                    if (!match_condition(msg, condition.first, condition.second)) {
                        match = false;
                        std::cout << "AND condition failed: " << condition.first << " = " << condition.second << "\n";
                        break;
                    }
                }
            } else if (query.operation == "OR") {
                for (const auto& condition : query.queries) {
                    std::cout << "Condition FIRST FOR OR: " << condition.first << std::endl;
                    std::cout << "Condition SECOND FOR OR: " << condition.second << std::endl;
                    if (match_condition(msg, condition.first, condition.second)) {
                        match = true;
                        std::cout << "OR condition matched: " << condition.first << " = " << condition.second << "\n";
                        break;
                    }
                }
            } else if (query.operation == "BOTH") {
                bool and_match = true;
                bool or_match = false;
                for (const auto& condition : query.queries) {
                    std::cout << "Condition FIRST FOR BOTH: " << condition.first << std::endl;
                    std::cout << "Condition SECOND FOR BOTH: " << condition.second << std::endl;
                    if (condition.first == "ismobile") {
                        if (!match_condition(msg, condition.first, condition.second)) {
                            and_match = false;
                            std::cout << "BOTH AND condition failed: " << condition.first << " = " << condition.second << "\n";
                            break;
                        }
                    } else {
                        if (match_condition(msg, condition.first, condition.second)) {
                            or_match = true;
                            std::cout << "BOTH OR condition matched: " << condition.first << " = " << condition.second << "\n";
                        }
                    }
                }
                match = and_match && or_match;
                std::cout << "BOTH condition result: AND match = " << and_match << ", OR match = " << or_match << "\n";
            }

            if (match) {
                if (!matched_ids.empty()) {
                    matched_ids += " ";
                }
                matched_ids += query.id;
                std::cout << "Query matched. Appending filter_id: " << query.id << "\n";
            }
        }

        if (matched_ids.empty()) {
            msg.filter_id = ShmemString("0", msg.filter_id.get_allocator());
            std::cout << "No query matched. Setting filter_id to 0\n";
        } else {
            msg.filter_id = ShmemString(matched_ids.c_str(), msg.filter_id.get_allocator());
            std::cout << "Final filter_id: " << matched_ids << "\n";
        }

        return matched_ids;
    }

    void printQuery() const {
        for (const auto& query : queries_) {
            std::cout << "Query ID: " << query.id << "\n";
            std::cout << "Operation: " << query.operation << "\n";
            for (const auto& condition : query.queries) {
                std::cout << "  " << condition.first << ": " << condition.second << "\n";
            }
            std::cout << "-------------------------\n";
        }
    }

private:
    bool match_condition(const UserMessages_Data& msg, const std::string& key, const std::string& value) const {
        if (key == "ismobile") {
            return std::to_string(msg.mobile_number) == value;
        } else if (key == "isIP") {
            return msg.ip_address == value.c_str();
        } else if (key == "isTTY") {
            return std::to_string(msg.transaction_type) == value;
        }
        return false;
    }
};

void read_and_log_messages(bip::managed_shared_memory& segment, 
                          std::vector<std::chrono::microseconds>& read_times,
                          size_t& last_processed_index,
                          std::vector<UserMessages_Data>& message_bundle,
                          const QueryFilter& query_filter) {
    typedef bip::allocator<char, bip::managed_shared_memory::segment_manager> CharAllocator;
    typedef bip::basic_string<char, std::char_traits<char>, CharAllocator> ShmemString;
    typedef bip::allocator<ShmemString, bip::managed_shared_memory::segment_manager> ShmemStringAllocator;
    typedef bip::vector<ShmemString, ShmemStringAllocator> ShmemVector;

    std::pair<ShmemVector*, bip::managed_shared_memory::size_type> result = segment.find<ShmemVector>("Messages");
    ShmemVector* messages = result.first;

    if (!messages) {
        std::cerr << "No messages found in shared memory." << std::endl;
        spdlog::error("No messages found in shared memory.");
        return;
    }

    std::cout << "Messages found in shared memory: " << messages->size() << std::endl;

    if (last_processed_index < messages->size()) {
        for (size_t i = last_processed_index; i < messages->size(); ++i) {
            auto& serialized_msg = (*messages)[i];
            auto start_time = std::chrono::high_resolution_clock::now();

            std::cout << "Processing message " << i << ": " << serialized_msg.c_str() << std::endl;

            // Deserialize the message
            std::istringstream iss(serialized_msg.c_str());
            boost::archive::text_iarchive ia(iss);
            UserMessages_Data msg(segment.get_segment_manager());

            try {
                ia >> msg;
            } catch (const std::exception& e) {
                std::cerr << "Deserialization error: " << e.what() << std::endl;
                spdlog::error("Deserialization error: {}", e.what());
                continue;
            }

            std::uint32_t received_checksum = compute_crc(msg);
            if (received_checksum != msg.checksum) {
                std::cerr << "Checksum mismatch for message with Transaction Type: " << msg.transaction_type << std::endl;
                spdlog::error("Checksum mismatch for message with Transaction Type: {}", msg.transaction_type);
                continue;
            }

            msg.filter_id = ShmemString(query_filter.match_query(msg).c_str(), msg.filter_id.get_allocator());
            message_bundle.push_back(msg);

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            read_times.push_back(duration);
        }

        last_processed_index = messages->size();
    }
}

bool DEBUGGING_MODE = false;

void dump_to_csv(const std::vector<UserMessages_Data>& messages, const std::string& filename) {
    std::ofstream csv_file(filename, std::ios::app); // Open in append mode
    if (csv_file.tellp() == 0) {
        csv_file << "Transaction Type,Creation Time,Mobile Number,IP Address,Personal Info,FilterID\n";
    }

    for (const auto& msg : messages) {
        json personal_info_json;
        for (const auto& entry : msg.personal_identifier_info) {
            personal_info_json[entry.first.c_str()] = entry.second.c_str();
        }
        std::string personal_info_str = personal_info_json.dump();
        // Remove the comma between Email and Name
        size_t pos = personal_info_str.find("\",\"");
        if (pos != std::string::npos) {
            personal_info_str.replace(pos, 3, "\" \"");
        }

        csv_file << msg.transaction_type << ","
                 << msg.creation_time << ","
                 << msg.mobile_number << ","
                 << msg.ip_address.c_str() << ","
                 << "\"" << personal_info_str << "\","
                 << msg.filter_id.c_str() << "\n";

        // Log and print the data being written to the CSV file
        std::cout << "Writing to CSV: " << msg.transaction_type << ", "
                  << msg.creation_time << ", "
                  << msg.mobile_number << ", "
                  << msg.ip_address.c_str() << ", "
                  << personal_info_str << ", "
                  << msg.filter_id.c_str() << std::endl;
        spdlog::info("Writing to CSV: {}, {}, {}, {}, {}, {}",
                     msg.transaction_type, msg.creation_time, msg.mobile_number, msg.ip_address.c_str(), personal_info_str, msg.filter_id.c_str());
    }

    csv_file.close();
}

int main() {
    loadConfig();
    LOGGER_LEVEL_SETTT = getConfigValue("LOGGER_LEVEL_SETTT");
    setup_logger("receiver_logger", "receiver_log.txt", LOGGER_LEVEL_SETTT);
    NAME_SHARED_MEMORY = getConfigValue("NAME_SHARED_MEMORY");
    const char* shared_memeory_name = NAME_SHARED_MEMORY.c_str();
    DEBUGGING_MODE = getConfigValue("DEBUGGING_MODE") == "true";
    std::string bundle_size = getConfigValue("BUNDLE_SIZE");
    std::string query_file_name = getConfigValue("QUERY_FILE_NAME");
    std::cout << "Current working directory: " << std::filesystem::current_path() << std::endl;
    std::string query_file_path = "../receiver/QueryFile/" + query_file_name; // Path to the query file

    std::cout << "Query file path: " << query_file_path << std::endl;

    QueryFilter qf(query_file_path);
    qf.printQuery();

    try {
        bip::managed_shared_memory segment(bip::open_only, shared_memeory_name);

        std::vector<std::chrono::microseconds> read_times;
        size_t last_processed_index = 0;
        std::vector<UserMessages_Data> message_bundle;
        std::string filename = getConfigValue("DUMP_CSV_FILE_NAME");
        std::string queue_name = getConfigValue("NAME_RABBIT_MQ");

        QueryFilter query_filter(query_file_path);

        while (true) {
            read_and_log_messages(segment, read_times, last_processed_index, message_bundle, query_filter);

            if (message_bundle.size() > 0) {
                send_to_rabbitmq(message_bundle, queue_name, bundle_size);
                if (DEBUGGING_MODE) {
                    dump_to_csv(message_bundle, filename);
                }
                message_bundle.clear();
            }

            int time_log = std::stoi(getConfigValue("LOG_TIMER_RECEIVER"));
            std::this_thread::sleep_for(std::chrono::seconds(time_log));
            std::thread log_thread(log_read_times, std::ref(read_times));
            log_thread.join();

            std::cout << "Waiting for new data in shared memory..." << std::endl;
            spdlog::info("Waiting for new data in shared memory...");
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        spdlog::error("Error: {}", e.what());
    }

    return 0;
}