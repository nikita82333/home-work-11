#include "gtest/gtest.h"

#include "MapReduce.h"

fs::path TEST_DIR{"./test_files/"};
fs::path TEMP{TEST_DIR/"temp/"};
fs::path OUT{TEST_DIR/"out/"};
std::size_t data_count;

void create_test_files() {
    fs::create_directory(TEST_DIR);
    fs::create_directory(TEMP);
    fs::create_directory(OUT);
    std::ofstream file(TEST_DIR/"emails.txt");
    file << "joshua5@hotmail.ru\n"
            "goshua5@hotmail.ru\n"
            "yog@sibmail.ru\n"
            "gvozdev_r@mail.ru\n"
            "alex-13@risp.ru\n"
            "pavstyuk@aport.ru\n"
            "Dj-Vovka@ngs.ru\n"
            "vetlan@rambler.ru\n"
            "lukaa@ngs.ru\n"
            "sorus@ok.ru \n"
            "leo_shubkin@mail.ru\n"
            "gurba_y@mail.ru\n"
            "beer_vodka@mail.ru\n"
            "K_Petrovich@ngs.ru\n"
            "geokot@mail.ru\n"
            "Inko_x@mail.ru\n"
            "a-kalinin@ngs.ru\n"
            "alexander.kalinin@ngs.ru\n"
            "wil-son@mail.ru\n"
            "viala@ngs.ru\n"
            "azanov@ngs.ru\n"
            "Lenusick@ngs.ru\n"
            "Ekaterina_26@list.ru\n"
            "Sib_nataly@ngs.ru\n"
            "alla_z@bk.ru\n"
            "skyer@mail.ru\n"
            "girevajaoa@ngs.ru\n"
            "lesyamag@mail.ru\n"
            "galeks2003@mail.ru\n"
            "lorndead@ngs.ru"
         << std::endl;
    file.close();
    data_count = 30;
}

TEST(SequentialWrite, test) {
    create_test_files();

    std::ifstream input(TEST_DIR/"emails.txt");
    int file_count = 5;
    int prefix_length = 1;

    std::vector<std::string> prefixes;
    std::string str;
    while (input >> str) {
        std::string prefix = str.substr(0, prefix_length);
        std::transform(prefix.begin(), prefix.end(), prefix.begin(), ::tolower);
        prefixes.push_back(prefix);
    }
    std::sort(prefixes.begin(), prefixes.end());
    {
        ShufflerFilePool pool(TEMP/"seq_test", file_count, std::ios::out, data_count);
        for (const auto& prefix : prefixes) {
            pool.sequential_write({prefix, "1"});
        }
    }
    std::vector<std::vector<Data>> result;
    {
        FilePool pool(TEMP/"seq_test", file_count, std::ios::in);
        for (int i = 0; i < file_count; ++i) {
            result.push_back(pool.read_all(i));
        }
    }
    std::vector<std::vector<Data>> expected {
            { {"a", "1"}, {"a", "1"}, {"a", "1"}, {"a", "1"}, {"a", "1"}, {"b", "1"} },
            { {"d", "1"}, {"e", "1"}, {"g", "1"}, {"g", "1"}, {"g", "1"}, {"g", "1"}, {"g", "1"}, {"g", "1"} },
            { {"i", "1"}, {"j", "1"}, {"k", "1"}, {"l", "1"}, {"l", "1"}, {"l", "1"}, {"l", "1"}, {"l", "1"} },
            { {"p", "1"}, {"s", "1"}, {"s", "1"}, {"s", "1"} },
            { {"v", "1"}, {"v", "1"}, {"w", "1"}, {"y", "1"} }
    };
    ASSERT_EQ(result, expected);
}

std::pair<int, int> run_mapreduce() {
    fs::path input{TEST_DIR/"emails.txt"};
    fs::path output{OUT};
    int mapper_count = 4, reducer_count = 3;
    int prefix_length = 1;
    MapReduce mapreduce(mapper_count, reducer_count, TEMP);
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
    return {mapper_count, reducer_count};
}

TEST(MapReduce, mapper_test) {
    auto [mapper_count, reducer_count] = run_mapreduce();
    std::vector<std::vector<Data>> result;
    {
        FilePool pool(TEMP/"mapper_out", mapper_count, std::ios::in);
        for (int i = 0; i < mapper_count; ++i) {
            result.push_back(pool.read_all(i));
        }
    }
    std::vector<std::vector<Data>> expected {
            { {"a", "1"}, {"d", "1"}, {"g", "1"}, {"g", "1"}, {"j", "1"}, {"p", "1"}, {"v", "1"}, {"y", "1"} },
            { {"b", "1"}, {"g", "1"}, {"g", "1"}, {"k", "1"}, {"l", "1"}, {"l", "1"}, {"s", "1"} },
            { {"a", "1"}, {"a", "1"}, {"a", "1"}, {"e", "1"}, {"i", "1"}, {"l", "1"}, {"v", "1"}, {"w", "1"} },
            { {"a", "1"}, {"g", "1"}, {"g", "1"}, {"l", "1"}, {"l", "1"}, {"s", "1"}, {"s", "1"} }
    };
    ASSERT_EQ(result, expected);
}

TEST(MapReduce, combiner_test) {
    auto [mapper_count, reducer_count] = run_mapreduce();
    std::vector<std::vector<Data>> result;
    {
        FilePool pool(TEMP/"combiner_out", mapper_count, std::ios::in);
        for (int i = 0; i < mapper_count; ++i) {
            result.push_back(pool.read_all(i));
        }
    }
    std::vector<std::vector<Data>> expected {
            { {"a", "1"}, {"d", "1"}, {"g", "2"}, {"j", "1"}, {"p", "1"}, {"v", "1"}, {"y", "1"} },
            { {"b", "1"}, {"g", "2"}, {"k", "1"}, {"l", "2"}, {"s", "1"} },
            { {"a", "3"}, {"e", "1"}, {"i", "1"}, {"l", "1"}, {"v", "1"}, {"w", "1"} },
            { {"a", "1"}, {"g", "2"}, {"l", "2"}, {"s", "2"} }
    };
    ASSERT_EQ(result, expected);
}

TEST(MapReduce, shuffler_test) {
    auto [mapper_count, reducer_count] = run_mapreduce();
    std::vector<std::vector<Data>> result;
    {
        FilePool pool(TEMP/"reducer_in", reducer_count, std::ios::in);
        for (int i = 0; i < reducer_count; ++i) {
            result.push_back(pool.read_all(i));
        }
    }
    std::vector<std::vector<Data>> expected {
            { {"a", "1"}, {"a", "3"}, {"a", "1"}, {"b", "1"}, {"d", "1"}, {"e", "1"}, {"g", "2"}, {"g", "2"}, {"g", "2"} },
            { {"i", "1"}, {"j", "1"}, {"k", "1"}, {"l", "2"}, {"l", "1"}, {"l", "2"} },
            { {"p", "1"}, {"s", "1"}, {"s", "2"}, {"v", "1"}, {"v", "1"}, {"w", "1"}, {"y", "1"} }
    };
    ASSERT_EQ(result, expected);
}

TEST(MapReduce, reducer_test) {
    auto [mapper_count, reducer_count] = run_mapreduce();
    std::vector<std::vector<Data>> result;
    {
        FilePool pool(OUT/"reducer_out", reducer_count, std::ios::in);
        for (int i = 0; i < reducer_count; ++i) {
            result.push_back(pool.read_all(i));
        }
    }
    std::vector<std::vector<Data>> expected {
            { {"false", "false"} },
            { {"false", "false"} },
            { {"false", "false"} }
    };
    ASSERT_EQ(result, expected);
}
