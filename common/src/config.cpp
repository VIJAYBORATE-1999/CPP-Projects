#include "config.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <filesystem>


std::string LOGGER_LEVEL_SETTT;
std::map<std::string, std::string> configMap;
std::string DATA_BULK_COUNT;
std::string NAME_SHARED_MEMORY;

void loadConfig() {
    try {
        std::cout << "Current working directory: " << std::filesystem::current_path() << std::endl;
        
        std::ifstream configFile("../config/values.config"); // Adjusted for running from build directory
        if (!configFile.is_open()) {
            throw std::runtime_error("Unable to open config file");
        }

        std::string line;
        while (std::getline(configFile, line)) {
            std::istringstream iss(line);
            std::string key, value;
            if (!(iss >> key >> value)) {
                std::cerr << "Skipping invalid line: " << line << std::endl;
                continue; // Skip lines that don't match the expected format
            }
            configMap[key] = value;
        }
        configFile.close();
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

std::string getConfigValue(const std::string& key, const std::string& defaultValue) {
    auto it = configMap.find(key);
    if (it != configMap.end()) {
        return it->second;
    } else {
        std::cerr << "Key not found: " << key << ". Using default value: " << defaultValue << std::endl;
        return defaultValue;
    }
}
