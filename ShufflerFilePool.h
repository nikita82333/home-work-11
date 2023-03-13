#ifndef SHUFFLERFILEPOOL_H
#define SHUFFLERFILEPOOL_H

#include "FilePool.h"

class ShufflerFilePool : public FilePool {
public:
    ShufflerFilePool(fs::path path, std::size_t files_count, std::ios_base::openmode mode, std::size_t data_count);

    void sequential_write(const Data& data);

private:
    std::size_t _data_count;
    std::size_t _data_index {0};
    std::size_t _file_index {0};
    std::size_t _file_size;
    std::size_t _size_exceeding {0};

    Data _prev_data;
    bool _next_file_pending {false};

};


#endif //SHUFFLERFILEPOOL_H
