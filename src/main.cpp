#include <iostream>
#include <functional>
#include <string>
#include <utility>
#include <vector>
#include <tuple>
#include <algorithm>
#include <numeric>
#include <fstream>
#include <random>
#include <limits>
#include <filesystem>
#include <chrono>
#include <optional>

using Number = int64_t;

size_t write_input_data(const std::string& filename, size_t file_size) {

    std::ofstream file(filename, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        std::cerr << "Error: (write input data) Unable to open file " << filename << std::endl;
        return 0;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<Number> dist(std::numeric_limits<Number>::min(), std::numeric_limits<Number>::max());
    // std::uniform_int_distribution<Number> dist(-20, 20);

    size_t n_elems = file_size / sizeof(Number);
    std::vector<Number> nums(n_elems);
    for (size_t i = 0; i < n_elems; i++) {
        nums[i] = dist(gen);
    }

    size_t current_size_byte = nums.size() * sizeof(Number);    
    file.write(reinterpret_cast<char *>(nums.data()), current_size_byte);

    file.close();
    std::cout << "File generated: " << filename << " with size: " << current_size_byte << " bytes " << " and " << n_elems << " elements" << std::endl;
    return n_elems;
}

void read_data(std::fstream& file, std::vector<Number>& nums, size_t start_element_file, size_t start_element_buffer, size_t read_elements) {
    std::streampos start_pos = start_element_file * sizeof(Number);
    size_t read_bytes = read_elements * sizeof(Number);

    file.seekg(start_pos);
    file.read(reinterpret_cast<char*>(nums.data() + start_element_buffer), read_bytes);
    // file.seekg(0);
}

void copy_file(std::string src_name, std::string dst_name) {
    std::ofstream file(dst_name, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        std::cerr << "Error: (copy file) Unable to open file " << dst_name << std::endl;
        return;
    }
    try {
        std::filesystem::copy(src_name, dst_name, std::filesystem::copy_options::overwrite_existing);
        // std::cout << "File copied successfully." << std::endl;
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "File copy failed: " << e.what() << std::endl;
    }
}

std::fstream file_open_and_clear(std::string filename) {
    std::fstream file_output(filename, std::ios::trunc | std::ios::binary | std::ios::in | std::ios::out);
    return file_output;
}

std::fstream file_open(std::string filename) {
    std::fstream file_input(filename, std::ios::binary | std::ios::in | std::ios::out);
    return file_input;
}


void print_block(std::fstream& file, size_t start_element, size_t block_size_elems) {
    std::vector<Number> block(block_size_elems);
    read_data(file, block, start_element, 0, block_size_elems);
    std::cout << "block starting at: " << start_element << " and ending at: " << start_element + block_size_elems << std::endl;
    for (auto elem : block) {
        std::cout << elem << ", ";
    }
    std::cout << std::endl;
}

void merge(std::vector<Number>& nums, size_t i_left, size_t i_mid, size_t i_right) {
    size_t left_size = i_mid - i_left + 1;
    size_t right_size = i_right - i_mid;

    std::vector<Number> left(nums.begin() + i_left, nums.begin() + i_mid + 1);
    std::vector<Number> right(nums.begin() + i_mid + 1, nums.begin() + i_right + 1);

    size_t i = 0;
    size_t j = 0;
    size_t k = i_left;
    while (i < left_size && j < right_size) {
        if (left[i] <= right[j]) {
            nums[k] = left[i];
            i++;
        }
        else {
            nums[k] = right[j];
            j++;
        }
        k++;
    } 

    while (i < left_size) {
        nums[k] = left[i];
        i++;
        k++;
    }

    while (j < right_size) {
        nums[k] = right[j];
        j++;
        k++;
    }
}

void merge_sort(std::vector<Number>& nums, size_t i_left, size_t i_right) {
    if (i_left >= i_right) return;

    size_t i_mid = i_left + (i_right- i_left) / 2;
    merge_sort(nums, i_left, i_mid);
    merge_sort(nums, i_mid + 1, i_right);
    merge(nums, i_left, i_mid, i_right);
}

void sort_internal(std::fstream& input, std::fstream& output, size_t internal_memory_size, size_t block_size, size_t& current_element_file, size_t max_element_file) {
    size_t max_elems_main = internal_memory_size / sizeof(Number);
    size_t max_elems_block = block_size / sizeof(Number);
    std::vector<Number> nums(max_elems_main);
    
    size_t current_block = 0;
    while (current_block < max_elems_main && current_element_file + max_elems_block < max_element_file) {
        read_data(input, nums, current_element_file, current_block, max_elems_block);
        current_element_file += max_elems_block;
        current_block += max_elems_block;
    }

    if (current_element_file + max_elems_block >= max_element_file) {
        size_t read_rest_size = max_element_file - current_element_file;
        read_data(input, nums, current_element_file, current_block, read_rest_size);
        current_element_file += read_rest_size;
        current_block += read_rest_size;
    }

    // shrink the vector if its not filled
    nums.resize(current_block);
    
    merge_sort(nums, 0, nums.size() - 1);
    // std::sort(nums.begin(), nums.end());
    
    size_t current_elem = 0;
    while (current_elem < max_elems_main) {
        output.write(reinterpret_cast<char*>(nums.data() + current_elem), max_elems_block * sizeof(Number));
        current_elem += max_elems_block;
    }
    size_t rest = max_elems_main - current_elem;
    if (rest > 0) {
        output.write(reinterpret_cast<char*>(nums.data() + current_elem), rest * sizeof(Number));
        current_elem += rest;
    }  
}

size_t partition(std::fstream& input, size_t file_size, std::fstream& output, size_t internal_memory_size, size_t block_size) {
    size_t current_element_file = 0;
    size_t max_element_file = file_size / sizeof(Number);
    size_t partitions = 0;
    while (current_element_file < max_element_file) {
        sort_internal(input, output, internal_memory_size, block_size, current_element_file, max_element_file);
        partitions++;
    }
    return partitions;
}

void debug_write(std::vector<Number> buffer, size_t start, size_t write_size_elements) {
    std::cout << "writing: " << std::endl;
    std::cout << "[";
    for (size_t i = start; i < write_size_elements; i++) {
        std::cout << buffer[i] << (i < write_size_elements - 1 ? ", " : "");
    }
    std::cout << "]" << std::endl;
}

/**
 * input here is the output file of the partition phase
 * output file is empty
 * chunks are the two consecutive sorted intervals in the previous output
 */
void external_merge_chunks(std::fstream& input, std::fstream& output, size_t internal_memory_size, size_t block_size, size_t iteration, size_t chunk_1_start, size_t chunk_2_start, size_t chunk_elements) {
    // at iteration 0 the sorted chunks in the file have the size internal_memory_size
    // the chunk size doubles with each iteration
    
    // chunk_1 and chunk_2 point to the current position in the input file
    size_t chunk_1 = chunk_1_start;
    size_t chunk_2 = chunk_2_start;
    size_t block_elements = block_size / sizeof(Number);

    std::vector<Number> block_1(block_elements);
    std::vector<Number> block_2(block_elements);
    std::vector<Number> output_buffer(block_elements);

    read_data(input, block_1, chunk_1, 0, block_elements);
    chunk_1 += block_elements;
    read_data(input, block_2, chunk_2, 0, block_elements);
    chunk_2 += block_elements;

    size_t i = 0, j = 0, k = 0;
    size_t elements_written = 0;
    while (elements_written < chunk_elements * 2) {
        while (i < block_elements && j < block_elements && k < block_elements) {
            if (block_1[i] <= block_2[j]) {
                output_buffer[k] = block_1[i];
                i++;
            }
            else {
                output_buffer[k] = block_2[j];
                j++;
            }
            k++;
        }

        // get a new block from file if i or j = block elements 
        // write output buffer to file if k = block elements
        if (i == block_elements) { 
            // dont read beginning of next chunk
            // cant get new block from chunk1 -> write all from chunk2
            if (chunk_1 < chunk_2_start) {
                read_data(input, block_1, chunk_1, 0, block_elements);
                chunk_1 += block_elements;
                i = 0;
            } else {
                std::fill(block_1.begin(), block_1.end(), std::numeric_limits<Number>::max());
                i = 0;
            }
        }
        else if (j == block_elements) {
            if (chunk_2 < chunk_2_start + chunk_elements) {
                read_data(input, block_2, chunk_2, 0, block_elements);
                chunk_2 += block_elements;
                j = 0;
            } else {
                std::fill(block_2.begin(), block_2.end(), std::numeric_limits<Number>::max());
                j = 0;
            }
        }
        else {
            output.write(reinterpret_cast<char*>(output_buffer.data()), block_elements * sizeof(Number));
            // debug_write(output_buffer, 0, block_elements);
            elements_written += block_elements;
            k = 0;
        }
    }  
    if (k > 0) {
        output.write(reinterpret_cast<char*>(output_buffer.data()), k * sizeof(Number));
    }
    
}

// merge chunks of possibly non equal size
void external_merge_chunks_neq(std::fstream& input, std::fstream& output, size_t internal_memory_size, size_t block_size,  size_t chunk_1_start, size_t chunk_1_size, size_t chunk_2_start, size_t chunk_2_size) {
    size_t chunk_1 = chunk_1_start;
    size_t chunk_2 = chunk_2_start;
    size_t block_elements = block_size / sizeof(Number);

    std::vector<Number> block_1(block_elements);
    std::vector<Number> block_2(block_elements);
    std::vector<Number> output_buffer(block_elements);

    read_data(input, block_1, chunk_1, 0, block_elements);
    chunk_1 += block_elements;
    read_data(input, block_2, chunk_2, 0, block_elements);
    chunk_2 += block_elements;

    size_t i = 0, j = 0, k = 0;
    size_t elements_written = 0;
    while (elements_written < chunk_1_size + chunk_2_size) {
        while (i < block_elements && j < block_elements && k < block_elements) {
            if (block_1[i] <= block_2[j]) {
                output_buffer[k] = block_1[i];
                i++;
            }
            else {
                output_buffer[k] = block_2[j];
                j++;
            }
            k++;
        }

        // get a new block from file if i or j = block elements 
        // write output buffer to file if k = block elements
        if (i == block_elements) { 
            // dont read beginning of next chunk
            // cant get new block from chunk1 -> write all from chunk2
            if (chunk_1 < chunk_1_start + chunk_1_size) {
                read_data(input, block_1, chunk_1, 0, block_elements);
                chunk_1 += block_elements;
                i = 0;
            } else {
                std::fill(block_1.begin(), block_1.end(), std::numeric_limits<Number>::max());
                i = 0;
            }
        }
        else if (j == block_elements) {
            if (chunk_2 < chunk_2_start + chunk_2_size) {
                read_data(input, block_2, chunk_2, 0, block_elements);
                chunk_2 += block_elements;
                j = 0;
            } else {
                std::fill(block_2.begin(), block_2.end(), std::numeric_limits<Number>::max());
                j = 0;
            }
        }
        else {
            output.write(reinterpret_cast<char*>(output_buffer.data()), block_elements * sizeof(Number));
            // debug_write(output_buffer, 0, block_elements);
            elements_written += block_elements;
            k = 0;
        }
    }  
    if (k > 0) {
        output.write(reinterpret_cast<char*>(output_buffer.data()), k * sizeof(Number));
    }

}

// merge all chunks in a file
void external_merge_file(std::fstream& input, std::fstream& output, size_t internal_memory_size, size_t block_size, size_t iteration, size_t& file_size_elements) {

    size_t chunk_elments = static_cast<size_t>(internal_memory_size / sizeof(Number) * pow(2, iteration)); // chunk size doubles at each iteration
    size_t chunk_1_start = 0;
    size_t chunk_2_start = chunk_1_start + chunk_elments;

    while (chunk_2_start + chunk_elments <= file_size_elements) {
        external_merge_chunks(input, output, internal_memory_size, block_size, iteration, chunk_1_start, chunk_2_start, chunk_elments); 
        chunk_1_start = chunk_2_start + chunk_elments;
        chunk_2_start = chunk_1_start + chunk_elments;
    }

    // odd number of chunks: keep last chunk till end and merge then
    // decrease file_size elements for subsequent iterations
    // check if difference to original in calling code
    // have to write last chunk from input to output every time
    if ((file_size_elements / chunk_elments) % 2 != 0) {
        file_size_elements = file_size_elements - chunk_elments;
    }
}

void copy_chunk(std::fstream& in, std::fstream& out, size_t start_elem, size_t end_elem, size_t block_size_elems) {
    size_t i = start_elem;
    while (i < end_elem) {
        std::vector<Number> buffer(block_size_elems);
        read_data(in, buffer, i, 0, block_size_elems);
        // out.seekp(i * sizeof(Number) );
        out.write(reinterpret_cast<char*>(buffer.data()), block_size_elems * sizeof(Number));
        // out.seekp(0);
        i += block_size_elems;
    }
}

/**
 * TODO: if initial partitions are odd, keep last chunk until end and merge it then
 */
std::fstream external_merge(std::string& filename_partitioned_input, std::string& filename_empty_output, size_t internal_memory_size, size_t block_size, size_t file_size_elements, size_t initial_partitions) {

    size_t max_iterations = static_cast<size_t>(floor(log2(initial_partitions)));
    size_t iteration = 0;

    size_t file_size_elements_orig = file_size_elements;
    std::fstream current_in = file_open(filename_partitioned_input);
    std::string current_in_name = filename_partitioned_input;
    std::fstream current_out = file_open_and_clear(filename_empty_output);
    std::string current_out_name = filename_empty_output;
    while (iteration < max_iterations) {
        external_merge_file(current_in, current_out, internal_memory_size, block_size, iteration, file_size_elements);
        // std::cout << "outfile after iteration: " << iteration << std::endl;
        
        if (file_size_elements_orig != file_size_elements) { 
            // write last chunk to current out
            // std::cout << "COPY CHUNK" << std::endl;
            copy_chunk(current_in, current_out, file_size_elements, file_size_elements_orig, block_size / sizeof(Number));
        }
        // std::cout << "current in: " << std::endl;
        // print_block(current_in, 0, file_size_elements_orig);
        // std::cout << "current out: " << std::endl;
        // print_block(current_out, 0, file_size_elements_orig);
        current_in.close();
        current_out.close();
        current_in_name.swap(current_out_name);
        current_in = file_open(current_in_name);
        current_out = file_open_and_clear(current_out_name);
        iteration++;
    }
    // merge remaining chunk
    if (file_size_elements_orig != file_size_elements) {
        external_merge_chunks_neq(current_in, current_out, internal_memory_size, block_size, 0, file_size_elements, file_size_elements, file_size_elements_orig - file_size_elements);
        current_in.close();
        return current_out;
    } else {
        current_out.close();
        return current_in;
    }   
}

void prepare(size_t filesize, std::string& in_filename, std::string& out_filename) {
    
    size_t n_elements = write_input_data(in_filename, filesize);    
    std::ofstream file_output(out_filename, std::ios::trunc);
    if (!file_output.is_open()) {
        std::cout << "file not opened: " << out_filename << std::endl;
    }
    file_output.close();
}



void test(std::fstream& file_unsorted, std::fstream& file_sorted, size_t file_element_size) {
    std::vector<Number> unsorted_nums(file_element_size);
    read_data(file_unsorted, unsorted_nums, 0, 0, file_element_size);
    std::sort(unsorted_nums.begin(), unsorted_nums.end());

    std::vector<Number> test_output(file_element_size);
    read_data(file_sorted, test_output, 0, 0, file_element_size);

    bool found_unequal = false;
    for (size_t i = 0; i < unsorted_nums.size(); i++) {
        auto a = unsorted_nums[i];
        auto b = test_output[i];
        if (a != b) {
            found_unequal = true;
            std::cout << "elements at index: " << i << " not equal. correct element: " << a << " incorrect element: " << b << std::endl;
        }
    }
    if (!found_unequal) {
        std::cout << "test passed" << std::endl;
    } else {
        std::cout << "test failed" << std::endl;
    }
}

std::optional<std::fstream> sort_file_external(std::string& in_filename, std::string& out_filename, size_t input_file_size, size_t internal_memory_size, size_t block_size) {
    
    std::fstream file_input = file_open(in_filename);
    if (!file_input.is_open()) {
        std::cerr << "Error: (sort external input) Unable to open file " << in_filename << std::endl;
        return std::nullopt;
    }
    
    std::fstream file_output = file_open_and_clear(out_filename);
    if (!file_output.is_open()) {
        std::cerr << "Error: (sort external output) Unable to open file " << out_filename << std::endl;
        return std::nullopt;
    }
    size_t initial_partitions = partition(file_input, input_file_size, file_output, internal_memory_size, block_size);
    std::cout << "initial partitions: " << initial_partitions << std::endl;

    // std::cout << "input" << std::endl;
    // print_block(file_input, 0, input_file_size / sizeof(Number));
    // std::cout << "partition output" << std::endl;
    // print_block(file_output, 0, input_file_size / sizeof(Number));

    // merge phase
    file_input.close();
    file_output.close();
    
    std::fstream result = external_merge(out_filename, in_filename, internal_memory_size, block_size, input_file_size / sizeof(Number), initial_partitions);
    return result;
}

void internal_mergesort_file(std::string& in_filename, std::string out_filename, size_t input_file_size) {
    copy_file(in_filename, std::string(DATA_PATH"original_input"));

    std::fstream in(in_filename);
    std::fstream out(out_filename);
    size_t file_elements = input_file_size / sizeof(Number);
    std::vector<Number> nums(file_elements);
    
    read_data(in, nums, 0, 0, file_elements);

    auto start_time = std::chrono::high_resolution_clock::now();
    merge_sort(nums, 0, nums.size() - 1);
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    if (duration < std::chrono::microseconds(1000)) {
        std::cout << "Duration: " << std::chrono::duration_cast<std::chrono::microseconds>(duration).count() << " µs\n";
    } else 
        std::cout << "Duration: " << std::chrono::duration_cast<std::chrono::milliseconds>(duration).count() << " ms\n";
    
    out.write(reinterpret_cast<char*>(nums.data()), input_file_size);    
    
    auto original_input = file_open(std::string(DATA_PATH"original_input"));
    test(original_input, out, input_file_size / sizeof(Number));

    original_input.close();
    in.close();
    out.close();
}


int main(int argc, char* argv[]) {

    std::string in_filename;
    std::string out_filename;
    size_t input_file_size;
    size_t block_size = 1024 * 1024 * 16;
    size_t internal_memory_size = 64 * 1024 * 1024;

    // if (argc > 1)
    //     std::cout << argv[1] << std::endl;

    if (argc > 1 && std::strcmp(argv[1], "gen-input") == 0) {
        if (argc > 4) {
            size_t input_file_size = std::stoul(argv[2]) * 1024 * 1024;
            in_filename = std::string(argv[3]);
            out_filename = std::string(argv[4]);
            prepare(input_file_size, in_filename, out_filename);
            return 0;
        }
        else {
            std::cout << "specify file size in MB and input and output filename" << std::endl;
            return 1;
        }
    }
    else if (argc > 1 && std::strcmp(argv[1], "sort-internal") == 0) {
        if (argc == 4) {
            in_filename = std::string(argv[2]);
            out_filename = std::string(argv[3]);
            std::string file_size_string;
            for (char ch : in_filename) {
                if (std::isdigit(ch)) {
                    file_size_string += ch;
                }
            }
            input_file_size = std::stoul(file_size_string) * 1024 * 1024;
            std::cout << "sorting file of " << file_size_string << " MB internally" << std::endl;            
            internal_mergesort_file(in_filename, out_filename, input_file_size);
            return 0;
        } else {
            std::cout << "specify two filenames" << std::endl;
            return 1;
        }
    }
    else if (argc > 1 && std::strcmp(argv[1], "sort-external") == 0) {
        if (argc == 5) {
            in_filename = std::string(argv[2]);
            out_filename = std::string(argv[3]);
            input_file_size = std::stoul(argv[4]) * 1024 * 1024;
        }
        else if (argc == 7) {
            in_filename = std::string(argv[2]);
            out_filename = std::string(argv[3]);
            input_file_size = std::stoul(argv[4]) * 1024 * 1024;
            block_size = std::stoul(argv[5]) * 1024 * 1024;
            internal_memory_size = std::stoul(argv[6]) * 1024 * 1024;
            std::cout << "block size: " << block_size / 1024 / 1024 << "  MB,  main memory size: " << internal_memory_size / 1024 / 1024 << " MB" << std::endl;
        }
        else {
            std::cout << "specify: input filename, output filename, input file size in MB. optionally: block size and main memory size" << std::endl;
            return 1;
        }
    }
    else {
        std::cout << "command line arguments are: " << std::endl;
        std::cout << "gen-input <filesize in MB> " << std::endl;
        std::cout << "sort-external <input_filename> <output_filename> (block size MB) (internal memory size MB)" << std::endl;
        std::cout << "sort-internal <input_filename> <output_filename>" << std::endl; 
        return 1;
    }

    
    copy_file(in_filename, std::string(DATA_PATH"input_copy"));
    
    auto copy_filename = std::string(DATA_PATH"input_copy");

    auto copyfile = file_open(copy_filename);
    // print_block(copyfile, 0, input_file_size / sizeof(Number));

    auto start_time = std::chrono::high_resolution_clock::now();

    auto result = sort_file_external(copy_filename, out_filename, input_file_size, internal_memory_size, block_size);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    if (duration < std::chrono::microseconds(1000)) {
        std::cout << "Duration: " << std::chrono::duration_cast<std::chrono::microseconds>(duration).count() << " µs\n";
    } else 
        std::cout << "Duration: " << std::chrono::duration_cast<std::chrono::milliseconds>(duration).count() << " ms\n";
    
    if (!result.has_value()) {
        std::cout << "sorting failed" << std::endl;
        return 1;
    }

    // print_block(result.value(), 0, input_file_size / sizeof(Number));

    auto input_unsorted = file_open(in_filename);
    test(input_unsorted, result.value(), input_file_size / sizeof(Number));

    input_unsorted.close();
    result.value().close();

    
    return 0;
}