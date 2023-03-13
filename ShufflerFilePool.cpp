#include <cmath>

#include "ShufflerFilePool.h"

ShufflerFilePool::ShufflerFilePool(fs::path path, std::size_t files_count,
                                   std::ios_base::openmode mode, std::size_t data_count)
        : FilePool(std::move(path), files_count, mode), _data_count(data_count) {
    _file_size = std::round(static_cast<long double>(_data_count) / files_count);
}

void ShufflerFilePool::sequential_write(const Data &data) {
    if (_data_index >= _file_size - _size_exceeding && !_next_file_pending && _file_index < _files_count - 1) {
        _size_exceeding = 0;
        _next_file_pending = true;
    }
    if (_next_file_pending) {
        if (data.key != _prev_data.key) {
            ++_file_index;
            _next_file_pending = false;
            _data_index = 0;
        } else {
            ++_size_exceeding;
        }
    }
    FilePool::write(_file_index, data);
    _prev_data = data;
    ++_data_index;
}
