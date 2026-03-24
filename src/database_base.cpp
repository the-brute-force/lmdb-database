#include "database.hpp"
#include <cstdint>
#include <utility>

namespace fs = std::filesystem;

bool Database::open(fs::path databasePath)
{
    bool usingDirectory = true;
    databasePath.make_preferred();

    fs::path databaseData;

    if (fs::is_regular_file(databasePath)) {
        usingDirectory = false;
        fs::path databaseLock = databasePath;

        if (!fs::exists(databaseLock+="-lock"))
            return false;

        databaseData = std::move(databasePath);
    } else if (fs::is_directory(databasePath)) {
        databaseData = databasePath / "data.mdb";

        if (!fs::is_regular_file(databaseData))
            return false;
    } else {
        return false;
    }

    // It's possible uintmax_t > size_t
    std::uintmax_t databaseSize = fs::file_size(databaseData);
    if (databaseSize > (std::size_t)-1)
        return false;

    MDB_env* newEnvironment = nullptr;
    if (mdb_env_create(&newEnvironment) != MDB_SUCCESS)
        return false;

    // Resize the map to be exactly the file size for minimal overhead
    if (mdb_env_set_mapsize(newEnvironment, static_cast<std::size_t>(databaseSize)) != MDB_SUCCESS) {
        mdb_env_close(newEnvironment);
        return false;
    }

    if (usingDirectory) {
        if (mdb_env_open(newEnvironment, databasePath.string().c_str(), MDB_NOTLS, 0664) != MDB_SUCCESS) {
            mdb_env_close(newEnvironment);
            return false;
        }
    } else {
        if (mdb_env_open(newEnvironment, databaseData.string().c_str(), MDB_NOSUBDIR & MDB_NOTLS, 0664) != MDB_SUCCESS) {
            mdb_env_close(newEnvironment);
            return false;
        }
    }

    // A temporary transaction needs to happen to get databaseIndex
    MDB_txn* temporaryTransaction = nullptr;
    if (mdb_txn_begin(newEnvironment, nullptr, 0, &temporaryTransaction) != MDB_SUCCESS) {
        mdb_env_close(newEnvironment);
        return false;
    }

    MDB_dbi newDatabaseIndex = 0;
    if (mdb_dbi_open(temporaryTransaction, nullptr, MDB_DUPSORT, &newDatabaseIndex) != MDB_SUCCESS) {
        mdb_txn_abort(temporaryTransaction);
        mdb_env_close(environment);
        return false;
    }

    mdb_txn_abort(temporaryTransaction);

    // Replace the environment only when a new working one exists
    std::lock_guard<std::mutex> lock(Database::mutex);

    if (Database::environment != nullptr) {
        mdb_dbi_close(Database::environment, Database::databaseIndex);
        mdb_env_close(Database::environment);
    }

    Database::databaseIndex = newDatabaseIndex;
    Database::environment = newEnvironment;
    Database::maxKeySize = mdb_env_get_maxkeysize(newEnvironment);

    return true;
}

std::shared_ptr<Database> Database::instance()
{
    static std::weak_ptr<Database> retainer;
    std::shared_ptr<Database> instance = retainer.lock();

    if (!instance) {
        retainer = instance = std::make_shared<Database>();
    }

    return instance;
}

Database::~Database()
{
    // This doesn't lock because it will only execute once
    if (Database::environment != nullptr) {
        mdb_dbi_close(Database::environment, Database::databaseIndex);
        mdb_env_close(Database::environment);
    }
}

bool createNewDatabase(fs::path newDatabaseDirectory)
{
    if (!fs::is_directory(newDatabaseDirectory))
        return false;

    MDB_env* environment;
    if (mdb_env_create(&environment) != MDB_SUCCESS)
        return false;

    if (mdb_env_open(environment, newDatabaseDirectory.make_preferred().string().c_str(), MDB_NOTLS, 0664) != MDB_SUCCESS) {
        mdb_env_close(environment);
        return false;
    }

    MDB_txn* transaction;
    if (mdb_txn_begin(environment, nullptr, 0, &transaction) != MDB_SUCCESS) {
        mdb_env_close(environment);
        return false;
    }

    MDB_dbi databaseIndex;
    if (mdb_dbi_open(transaction, nullptr, MDB_CREATE, &databaseIndex) != MDB_SUCCESS) {
        mdb_txn_abort(transaction);
        mdb_env_close(environment);
        return false;
    }

    bool successfullyCreated = (mdb_txn_commit(transaction) == MDB_SUCCESS);

    mdb_dbi_close(environment, databaseIndex);
    mdb_env_close(environment);

    return successfullyCreated;
}
