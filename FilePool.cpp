#include "FilePool.h"

FilePool::FilePool(fs::path path, std::size_t files_count, std::ios_base::openmode mode)
    : _mode(mode), _path(std::move(path)), _files_count(files_count) {
    _file_pool.resize(_files_count);
    fs::path filename = _path.filename();
    fs::path dir = _path.remove_filename();
    for (std::size_t i = 0; i < _files_count; ++i) {
        _file_pool[i].open(dir/(filename.string() + std::to_string(i)), mode);
        if (!_file_pool[i].is_open()) {
            std::cerr << "File opening error: " << filename.string() + std::to_string(i) << std::endl;
        }
    }
}

FilePool::~FilePool() {
    for (auto& file : _file_pool) {
        file.close();
    }
}

void FilePool::write(std::size_t index, const Data& data) {
    if (index < _file_pool.size() && (std::ios::out & _mode)) {
        _file_pool[index] << data.key << " " << data.value << std::endl;
    }
}

void FilePool::write(std::size_t index, const std::vector<Data> &v_data) {
    if (index < _file_pool.size() && (std::ios::out & _mode)) {
        for (const auto& data : v_data)
        _file_pool[index] << data.key << " " << data.value << std::endl;
    }
}

Data FilePool::read(std::size_t index) {
    Data data;
    if (index < _file_pool.size() && (std::ios::in & _mode)) {
        _file_pool[index] >> data.key >> data.value;
    }
    return data;
}

std::vector<Data> FilePool::read_all(std::size_t index) {
    std::vector<Data> v_data;
    if (index < _file_pool.size() && (std::ios::in & _mode)) {
        Data data;
        while (_file_pool[index] >> data.key >> data.value) {
            v_data.push_back(data);
        }
    }
    return v_data;
}

void FilePool::close(std::size_t index) {
    _file_pool[index].close();
}
