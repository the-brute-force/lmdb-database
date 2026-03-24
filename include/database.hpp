#pragma once
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

extern "C" {
#include <lmdb.h>
}

class Database {
// All methods check the static variables to fail gracefully
// Some methods use const-overloading to allow for a constant instance
public:
    std::optional<std::vector<std::string>> at(const std::string& key) const;
    inline std::optional<std::vector<std::string>> operator[](const std::string& key) const
    {
        return at(key);
    }
    
    std::optional<bool> contains(const std::string& key) const;
    
    bool erase(const std::string& key);
    bool erase(const std::string&) const = delete;
    
    bool insert(const std::string& key, const std::vector<std::string>& values);
    bool insert(const std::string&, const std::vector<std::string>&) const = delete;
    
    // Convenience method for single insertions
    inline bool insert(const std::string& key, const std::string& value)
    {
        return insert(key, std::vector<std::string>{value});
    }
    bool insert(const std::string&, const std::string&) const = delete;
    
    static std::shared_ptr<Database> instance();
    
    // This sets up the environment, is can be used to recover from MDB_PANIC
    bool open(std::filesystem::path databasePath);
    bool open(std::filesystem::path) const = delete;
    
    Database(const Database&) = delete;
    void operator=(const Database&) = delete;

    friend std::allocator<Database>;
    friend std::default_delete<Database>;
protected:
    inline static std::mutex mutex;
    inline static MDB_dbi databaseIndex = 0;
    inline static MDB_env* environment = nullptr;
    inline static int maxKeySize = 0;
private:
    Database() = default;
    ~Database();
};

bool createNewDatabase(std::filesystem::path newDatabaseDirectory);
