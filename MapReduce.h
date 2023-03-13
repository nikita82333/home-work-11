#include <vector>
#include <functional>
#include <future>
#include <fstream>
#include <numeric>

#include "FilePool.h"
#include "ShufflerFilePool.h"


using mapper_type = std::function<Data(const std::string&)>;
using combiner_type = std::function<Data(const Data&, Data&)>;
using reducer_type = std::function<Data(const Data&, const Data&)>;

class MapReduce {
public:

    MapReduce(int mappers_count, int reducers_count, fs::path work = {"./work/"});

    void run(const fs::path& input, const fs::path& output);
    void set_mapper(mapper_type mapper);
    void set_combiner(combiner_type combiner);
    void set_reducer(reducer_type reducer);
    std::string get_output_filename();

private:
    struct Block {
        std::size_t from;
        std::size_t to;
    };

    static std::vector<Block> split_file(const fs::path& path, std::size_t blocks_count);
    std::size_t run_mappers(const std::vector<Block>& blocks, const fs::path& input);
    std::size_t run_combiners();
    void run_shuffler(std::size_t data_size) const;
    void run_reducers(const fs::path& output);

    std::size_t _mappers_count;
    std::size_t _reducers_count;

    mapper_type _mapper;
    combiner_type _combiner;
    reducer_type _reducer;

    const std::string _mapper_out {"mapper_out"};
    const std::string _combiner_out {"combiner_out"};
    const std::string _reducer_in {"reducer_in"};
    const std::string _reducer_out {"reducer_out"};
    fs::path _work;

};