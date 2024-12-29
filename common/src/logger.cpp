#include "logger.h"
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <filesystem>  // Ensure this header is included
#include <unordered_map>
#include <iostream>

void setup_logger(const std::string &logger_name, const std::string &file_name, const std::string &log_level_str) {
    std::filesystem::path log_dir = std::filesystem::current_path() / "log";
    std::filesystem::create_directories(log_dir);
    std::filesystem::path log_file = log_dir / file_name;

    auto logger = spdlog::basic_logger_mt(logger_name, log_file.string());
    spdlog::set_default_logger(logger);
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S] [%l] [%s:%#] %v");

    static const std::unordered_map<std::string, spdlog::level::level_enum> log_levels = {
        {"trace", spdlog::level::trace},
        {"debug", spdlog::level::debug},
        {"info", spdlog::level::info},
        {"warn", spdlog::level::warn},
        {"error", spdlog::level::err},
        {"critical", spdlog::level::critical},
        {"off", spdlog::level::off}
    };

    auto it = log_levels.find(log_level_str);
    if (it != log_levels.end()) {
        spdlog::set_level(it->second);
        std::string level_str(spdlog::level::to_string_view(it->second).data(), spdlog::level::to_string_view(it->second).size());
        std::cout << "LOG LEVEL " << log_level_str << " == " << level_str << std::endl;
    } else {
        spdlog::set_level(spdlog::level::info);
        std::cout << "LOG LEVEL INVALID: " << log_level_str << std::endl;
        spdlog::warn("Invalid log level '{}', defaulting to 'info'", log_level_str);
    }

    spdlog::level::level_enum current_level = spdlog::get_level();
    std::string current_level_str(spdlog::level::to_string_view(current_level).data(), spdlog::level::to_string_view(current_level).size());
    std::cout << "Current log level: " << current_level_str << std::endl;
}