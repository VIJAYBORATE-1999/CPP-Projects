cmake_minimum_required(VERSION 3.10)
project(Assignment_4)

set(CMAKE_CXX_STANDARD 17)

# Add debugging symbols
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
set(CMAKE_BUILD_TYPE Debug)

# Add local Boost and spdlog headers
include_directories(${CMAKE_SOURCE_DIR}/lib/Boost_Headers)
include_directories(${CMAKE_SOURCE_DIR}/lib/spdlog)

# Include directories for headers
include_directories(
    lib
    common/include
    lib/spdlog
    ${CMAKE_SOURCE_DIR}/lib/rabbitmq_Headers  # Add RabbitMQ headers
    ${CMAKE_SOURCE_DIR}/lib/nlohmann_Json_Header  # Add JSON header
)

# Add local Boost, spdlog, and RabbitMQ libraries
link_directories(${CMAKE_SOURCE_DIR}/lib/Boost_libraries)
link_directories(${CMAKE_SOURCE_DIR}/lib/spdlog_libraries)
link_directories(${CMAKE_SOURCE_DIR}/lib/rabbitmq_Libraries)  # Add RabbitMQ libraries

# Include the directory where UserMessages_Data.h is located
include_directories(${CMAKE_SOURCE_DIR}/messageclass/src)

# Add the sender and receiver executables
add_executable(sender sender/src/sender.cpp common/src/logger.cpp common/src/config.cpp)
add_executable(receiver receiver/src/receiver.cpp common/src/logger.cpp common/src/config.cpp)

# Link Boost, spdlog, and RabbitMQ libraries
target_link_libraries(sender boost_system boost_filesystem boost_serialization spdlog rabbitmq)
target_link_libraries(receiver boost_system boost_filesystem boost_serialization spdlog rabbitmq)