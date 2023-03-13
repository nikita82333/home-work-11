#include <iostream>
#include <filesystem>

#include "MapReduce.h"

namespace fs = std::filesystem;

int main(int argc, char** argv) {

    if (argc != 4) {
        std::cerr << "Usage: mapreduce <src> <mnum> <rnum>" << std::endl;
        std::cerr << " - <src> - source file path" << std::endl;
        std::cerr << " - <mnum> - number of threads to map" << std::endl;
        std::cerr << " - <rnum> - number of threads to reduce" << std::endl;

        return 1;
    }

    fs::path input{argv[1]};
    if (!fs::exists(input)) {
        std::cerr << "Source file is not exists!" << std::endl;
        return 1;
    }

    try {
        fs::path output{"./out/"};
        fs::create_directory(output);
        int mappers_count = std::stoi(argv[2]);
        int reducers_count = std::stoi(argv[3]);

        MapReduce mapreduce(mappers_count, reducers_count);

        int prefix_length = 1;
        while (true) {
            mapreduce.set_mapper([prefix_length](const std::string &input) -> Data {
                std::string prefix = input.substr(0, prefix_length);
                std::transform(prefix.begin(), prefix.end(), prefix.begin(), ::tolower);
                return {prefix, "1"};
            });
            mapreduce.set_combiner([](const Data &data, Data &temp) -> Data {
                if (temp.key.empty()) {
                    temp = data;
                    return {};
                } else {
                    if (data.key == temp.key) {
                        temp.value = std::to_string(std::stoi(temp.value) + std::stoi(data.value));
                        return {};
                    } else {
                        Data result = temp;
                        temp = data;
                        return result;
                    }
                }
            });
            mapreduce.set_reducer([](const Data &prev, const Data &data) -> Data {
                if (prev.value.empty() || prev.value == "true") {
                    if (data.key == prev.key || std::stoi(data.value) > 1) {
                        return {"false", "false"};
                    } else {
                        return {data.key, "true"};
                    }
                } else {
                    return {"false", "false"};
                }
            });

            mapreduce.run(input, output);
            FilePool out(output/mapreduce.get_output_filename(), reducers_count, std::ios::in);
            bool success {true};
            for (int i = 0; i < reducers_count; ++i) {
                Data data = out.read(i);
                if (data.value == "false") {
                    success = false;
                    break;
                }
            }
            if (success) {
                break;
            }
            ++prefix_length;
        }
        std::cout << "Minimal prefix length = " << prefix_length << std::endl;
    } catch (const std::exception& exception) {
        std::cerr << "Exception: " << exception.what() << std::endl;
    }

    return 0;
}
