#include "MapReduce.h"

MapReduce::MapReduce(int mappers_count, int reducers_count, fs::path work)
        : _mappers_count(mappers_count), _reducers_count(reducers_count), _work(std::move(work)) {
    if (!fs::exists(_work)) {
        fs::create_directory(_work);
    }
}

void MapReduce::run(const fs::path& input, const fs::path& output) {
    auto blocks = split_file(input, _mappers_count);

    run_mappers(blocks, input);

    std::size_t data_size = run_combiners();

    run_shuffler(data_size);

    run_reducers(output);
}

void MapReduce::set_mapper(mapper_type mapper) {
    _mapper = std::move(mapper);
}

void MapReduce::set_combiner(combiner_type combiner) {
    _combiner = std::move(combiner);
}

void MapReduce::set_reducer(reducer_type reducer) {
    _reducer = std::move(reducer);
}

std::string MapReduce::get_output_filename() {
    return _reducer_out;
}

std::vector<MapReduce::Block> MapReduce::split_file(const fs::path& path, std::size_t blocks_count) {
    std::uintmax_t file_size = fs::file_size(path);
    std::size_t block_size = file_size / blocks_count;

    std::vector<Block> blocks(blocks_count);
    std::size_t low_boundary = 0, high_boundary;
    std::ifstream input_file(path, std::ios::binary);
    for (std::size_t i = 1; i < blocks_count; ++i) {
        input_file.seekg(i * block_size);
        char ch;
        do {
            input_file.read(&ch, sizeof(char));
        } while (ch != '\n');
        high_boundary = input_file.tellg() - std::ifstream::pos_type{sizeof(char)};
        blocks[i - 1] = {low_boundary, high_boundary};
        low_boundary = input_file.tellg();
    }
    blocks[blocks_count - 1] = {low_boundary, file_size - 1};
    input_file.close();

    return blocks;
}

std::size_t MapReduce::run_mappers(const std::vector<Block>& blocks, const fs::path& input) {
    FilePool mapper_out(_work/_mapper_out, _mappers_count, std::ios::out);
    std::vector<std::future<std::size_t>> mappers_futures(_mappers_count);
    for (std::size_t i_mapper = 0; i_mapper < _mappers_count; ++i_mapper) {
        mappers_futures[i_mapper] = std::async(std::launch::async, [&, i_mapper]() {
            std::ifstream input_file(input, std::ios::binary);
            std::vector<Data> result;
            char ch;
            std::string line;
            input_file.seekg(blocks[i_mapper].from);
            for (std::size_t i = blocks[i_mapper].from; i <= blocks[i_mapper].to; ++i) {
                input_file.read(&ch, sizeof(char));
                if (ch != '\n') {
                    if (ch != '\r') {
                        line += ch;
                    }
                } else {
                    result.push_back(_mapper(line));
                    line.clear();
                }
            }
            input_file.close();

            std::sort(result.begin(), result.end(),
                      [](const Data& a, const Data& b) {return a.key < b.key;});

            mapper_out.write(i_mapper, result);

            return std::size_t {result.size()};
        });
    }

    return std::accumulate(mappers_futures.begin(), mappers_futures.end(), 0,
                           [](std::size_t a, std::future<std::size_t>& b) {return a + b.get();});
}

std::size_t MapReduce::run_combiners() {
    FilePool mapper_out(_work/_mapper_out, _mappers_count, std::ios::in);
    FilePool combiner_out(_work/_combiner_out, _mappers_count, std::ios::out);
    std::vector<std::future<std::size_t>> combiners_futures(_mappers_count);
    for (std::size_t i = 0; i < _mappers_count; ++i) {
        combiners_futures[i] = std::async(std::launch::async, [&, i]() {
            std::size_t count = 0;
            Data result, temp;
            for (Data data = mapper_out.read(i); !data.key.empty();) {
                result = _combiner(data, temp);
                if (!result.key.empty()) {
                    combiner_out.write(i, result);
                    ++count;
                }
                data = mapper_out.read(i);
            }
            if (!temp.key.empty()) {
                combiner_out.write(i, temp);
                ++count;
            }
            return count;
        });
    }

    return std::accumulate(combiners_futures.begin(), combiners_futures.end(), 0,
                           [](std::size_t a, std::future<std::size_t>& b) {return a + b.get();});
}

void MapReduce::run_shuffler(std::size_t data_size) const {
    struct FileData {
        Data data;
        std::size_t file_index;
    };

    FilePool combiner_out(_work/_combiner_out, _mappers_count, std::ios::in);
    ShufflerFilePool reducer_in(_work/_reducer_in, _reducers_count, std::ios::out, data_size);
    std::vector<FileData> buffer;
    for (std::size_t i = 0; i < _mappers_count; ++i) {
        buffer.push_back(FileData{combiner_out.read(i), i});
    }

    while (!buffer.empty()) {
        auto min_it = std::min_element(buffer.begin(), buffer.end(),
            [](const FileData &a, const FileData &b) {return a.data.key < b.data.key;});

        reducer_in.sequential_write(min_it->data);

        min_it->data = combiner_out.read(min_it->file_index);
        if (min_it->data.key.empty()) {
            combiner_out.close(min_it->file_index);
            buffer.erase(min_it);
        }
    }
}

void MapReduce::run_reducers(const fs::path& output) {
    FilePool reducer_in(_work/_reducer_in, _reducers_count, std::ios::in);
    FilePool reducer_out(output/_reducer_out, _reducers_count, std::ios::out);
    std::vector<std::future<void>> reducers_futures(_reducers_count);
    for (std::size_t i_reducer = 0; i_reducer < _reducers_count; ++i_reducer) {
        reducers_futures[i_reducer] = std::async(std::launch::async, [&, i_reducer]() {
            Data result;
            for (Data data = reducer_in.read(i_reducer); !data.key.empty();) {
                result = _reducer(result, data);
                data = reducer_in.read(i_reducer);
            }
            reducer_out.write(i_reducer, result);
        });
    }

    for (auto& future : reducers_futures) {
        future.wait();
    }
}
