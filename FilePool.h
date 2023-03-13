#ifndef FILEPOOL_H
#define FILEPOOL_H

#include <filesystem>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>

namespace fs = std::filesystem;

struct Data {
    std::string key;
    std::string value;
    bool operator ==(const Data& other) const {
        return (key == other.key &&
                value == other.value);
    }

};

class FilePool {
public:
    FilePool(fs::path path, std::size_t files_count, std::ios_base::openmode mode);
    virtual ~FilePool();

    void write(std::size_t index, const Data& data);
    void write(std::size_t index, const std::vector<Data>& v_data);

    Data read(std::size_t index);
    std::vector<Data> read_all(std::size_t index);
    void close(std::size_t index);

private:
    std::vector<std::fstream> _file_pool;
    std::ios_base::openmode _mode;
    fs::path _path;

protected:
    std::size_t _files_count;

};


#endif //FILEPOOL_H
