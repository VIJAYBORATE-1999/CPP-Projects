#ifndef CONFIG_H
#define CONFIG_H

#include <string>
#include <map>

extern std::string LOGGER_LEVEL_SETTT;
extern std::map<std::string, std::string> configMap;
extern std::string DATA_BULK_COUNT;
extern std::string NAME_SHARED_MEMORY;

void loadConfig();
std::string getConfigValue(const std::string& key, const std::string& defaultValue = "");

#endif // CONFIG_H