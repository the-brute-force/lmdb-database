#include "database.hpp"
#include <cstdint>
#include <utility>

#if __WORDSIZE == 32
static constexpr std::size_t MDB_MAX_MAPSIZE = ((std::size_t)2 << 29);
#elif __WORDSIZE == 64
static constexpr std::size_t MDB_MAX_MAPSIZE = ((std::size_t)2 << 39);
#else
# error "Unknown value for __WORDSIZE"
#endif

std::optional<std::vector<std::string>> Database::at(const std::string& key) const
{
    std::lock_guard<std::mutex> lock(Database::mutex);
    if (Database::environment == nullptr)
        return std::nullopt;

    if (key.length() > Database::maxKeySize)
        return std::nullopt;

    MDB_val nativeKey;
    nativeKey.mv_size = key.length();
    nativeKey.mv_data = const_cast<char*>(key.c_str());

    std::vector<std::string> retrievedValues;

    MDB_txn* transaction;
    if (mdb_txn_begin(Database::environment, nullptr, MDB_RDONLY, &transaction) != MDB_SUCCESS)
        return std::nullopt;

    MDB_cursor* cursor;
    if (mdb_cursor_open(transaction, Database::databaseIndex, &cursor) != MDB_SUCCESS) {
        mdb_txn_abort(transaction);
        return std::nullopt;
    }

    MDB_val nativeValue;

    // Move the cursor to the first instance and get its value
    int error = mdb_cursor_get(cursor, &nativeKey, &nativeValue, MDB_SET_KEY);
    if (error != MDB_SUCCESS) {
        mdb_cursor_close(cursor);
        mdb_txn_abort(transaction);

        if (error == MDB_NOTFOUND)
            return retrievedValues;

        return std::nullopt;
    }

    std::string firstValue(static_cast<const char*>(nativeValue.mv_data), nativeValue.mv_size);
    retrievedValues.push_back(std::move(firstValue));

    // This could be a while-true statement, but this is technically safer
    while (error == MDB_SUCCESS) {
        if ((error = mdb_cursor_get(cursor, &nativeKey, &nativeValue, MDB_NEXT_DUP)) != MDB_SUCCESS)
            break;

        std::string value(static_cast<const char*>(nativeValue.mv_data), nativeValue.mv_size);
        retrievedValues.push_back(std::move(value));
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(transaction);

    return retrievedValues;
}

std::optional<bool> Database::contains(const std::string& key) const
{
    std::lock_guard<std::mutex> lock(Database::mutex);
    if (Database::environment == nullptr)
        return std::nullopt;

    if (key.length() > Database::maxKeySize)
        return false;

    MDB_val nativeKey;
    nativeKey.mv_size = key.length();
    nativeKey.mv_data = const_cast<char*>(key.c_str());

    MDB_txn* transaction;
    if (mdb_txn_begin(Database::environment, nullptr, MDB_RDONLY, &transaction) != MDB_SUCCESS)
        return std::nullopt;

    // An output must be provided, so this dummy value is used
    // This doesn't need any cleanup since it points to the memory map
    MDB_val nativeValue;

    int error = mdb_get(transaction, Database::databaseIndex, &nativeKey, &nativeValue);
    mdb_txn_abort(transaction);

    if (error == MDB_SUCCESS) {
        return true;
    } else if (error == MDB_NOTFOUND) {
        return false;
    }

    return std::nullopt;
}

bool Database::erase(const std::string& key)
{
    std::lock_guard<std::mutex> lock(Database::mutex);
    if (Database::environment == nullptr)
        return false;

    if (key.length() > Database::maxKeySize)
        return false;

    MDB_val nativeKey;
    nativeKey.mv_size = key.length();
    nativeKey.mv_data = const_cast<char*>(key.c_str());

    MDB_txn* transaction;
    if (mdb_txn_begin(Database::environment, nullptr, 0, &transaction) != MDB_SUCCESS)
        return false;

    int error = mdb_del(transaction, Database::databaseIndex, &nativeKey, nullptr);

    if (error == MDB_SUCCESS) {
        mdb_txn_commit(transaction);
    } else {
        mdb_txn_abort(transaction);
    }

    return (error == MDB_SUCCESS);
}

bool Database::insert(const std::string& key, const std::vector<std::string>& values)
{
    if (values.size() == 0)
        return false;
    
    std::lock_guard<std::mutex> lock(Database::mutex);
    if (Database::environment == nullptr)
        return false;
    
    if (key.length() > Database::maxKeySize)
        return false;

    for (const std::string& value : values) {
        if (value.length() > Database::maxKeySize)
            return false;
    }
    
    MDB_val nativeKey;
    nativeKey.mv_size = key.length();
    nativeKey.mv_data = const_cast<char*>(key.c_str());

    MDB_envinfo info;
    mdb_env_info(Database::environment, &info);

    // Attempt to resize map to its max before transacting
    if (mdb_env_set_mapsize(Database::environment, MDB_MAX_MAPSIZE) != MDB_SUCCESS)
        return false;

    MDB_txn* transaction = nullptr;
    int error = mdb_txn_begin(Database::environment, nullptr, 0, &transaction);

    if (error != MDB_SUCCESS) {
        mdb_env_set_mapsize(Database::environment, info.me_mapsize);
        return false;
    }

    for (const std::string& value : values) {
        MDB_val nativeValue;
        nativeValue.mv_size = value.length();
        nativeValue.mv_data = const_cast<char*>(value.c_str());
        
        if ((error = mdb_put(transaction, Database::databaseIndex, &nativeKey, &nativeValue, 0)) != MDB_SUCCESS)
            break;
    }

    if (error == MDB_SUCCESS) {
        // If the data has been commit, resize if the old map size is too small
        if ((error = mdb_txn_commit(transaction)) == MDB_SUCCESS) {
            MDB_stat stat;
            mdb_env_stat(Database::environment, &stat);

            std::size_t bytesUsed = (stat.ms_branch_pages + stat.ms_leaf_pages + stat.ms_overflow_pages) * stat.ms_psize;

            if (info.me_mapsize < bytesUsed)
                info.me_mapsize = bytesUsed;
        }
    } else {
        mdb_txn_abort(transaction);
    }

    mdb_env_set_mapsize(Database::environment, info.me_mapsize);
    return (error == MDB_SUCCESS);
}
