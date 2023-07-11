#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <mpi.h>
#include <string>
#include <sstream>
#include <vector>
#include <ctype.h>
#include <unordered_map>
#include <algorithm>
#include "utils.h"

using namespace std;

void process_stream(int comm_size, string words, int length, int *checkpoints, int rank, int min_length, int max_length)
{

    string substring;
    int partial_length;
    
    // send the substring based on checkpoints
    if (!rank)
    {

        for (int i = 1; i < comm_size; i++)
        {
            substring = words.substr(checkpoints[i], checkpoints[i+1] - checkpoints[i]);
            MPI_Send(substring.c_str(), checkpoints[i + 1] - checkpoints[i], MPI_CHAR, i, 0, MPI_COMM_WORLD);

        }

        // rank 0 substring
        partial_length = checkpoints[1] - checkpoints[0];
        substring = words.substr(checkpoints[0], checkpoints[1] - checkpoints[0]);
    }

    // receive the substring
    if (rank)
    {
        // get size (https://mpitutorial.com/tutorials/dynamic-receiving-with-mpi-probe-and-mpi-status/)
        MPI_Status status;
        MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &partial_length);
        char* receive = (char *)malloc(partial_length + 1);
        receive[partial_length] = '\0';
        MPI_Recv(receive, partial_length, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        substring.assign(receive);
    }

    // get the words from the substring and serialize them
    serialized_map partial = serialize_map(get_words(substring, partial_length, min_length, max_length), rank);

    //send the serialized code
    if (rank)
    {
        // send concatenated words
        MPI_Send(partial.words.c_str(), partial.total_char_length, MPI_CHAR, 0, 1, MPI_COMM_WORLD);

        // send word length array
        MPI_Send(partial.word_length, partial.total_words_length, MPI_INT, 0, 2, MPI_COMM_WORLD);

        // send word frequency array
        MPI_Send(partial.word_freq, partial.total_words_length, MPI_INT, 0, 3, MPI_COMM_WORLD);
    }
    else // received the serialized code and process (rank 0)
    {
        string words;
        int source;
        int *word_length, *word_freq;

        array<unordered_map<std::string, int>, 4> map_arr;
        unordered_map<std::string, int> combined_map;
        for (int i = 1; i < comm_size; i++)
        {
            serialized_map local_partial;
            MPI_Status status;
            int _count;

            // receive the serialized word string
            MPI_Probe(MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_CHAR, &_count);
            source = status.MPI_SOURCE;
            char* temp = (char *)malloc(_count + 1);
            temp[_count] = '\0';
            MPI_Recv(temp, _count, MPI_CHAR, source, 1, MPI_COMM_WORLD, &status);
            words.assign(temp);
            local_partial.total_char_length = _count;

            // receive the array of word size
            MPI_Probe(source, 2, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_INT, &_count);
            word_length = (int *)malloc(sizeof(int) * _count);
            MPI_Recv(word_length, _count, MPI_INT, source, 2, MPI_COMM_WORLD, &status);
            local_partial.total_words_length = _count;

            // receive the array of word frequency
            MPI_Probe(source, 3, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_INT, &_count);
            word_freq = (int *)malloc(sizeof(int) * _count);
            MPI_Recv(word_freq, _count, MPI_INT, source, 3, MPI_COMM_WORLD, &status);

            local_partial.word_length = word_length;
            local_partial.word_freq = word_freq;
            local_partial.words = words;

            // deserialize the serialized map into an unordered map
            map_arr[source] = deserialize_map(local_partial);
        }

        // deserialized for rank 0
        map_arr[0] = deserialize_map(partial);

        // reduce the maps received from all ranks
        for (int i = 0; i < comm_size; i++)
        {
            // cout << map_arr[i].size() << endl;
            for (const auto &pair : map_arr[i])
            {
                // cout<< "rank "<<i<<" "<<pair.first<<" "<<pair.second<<endl;
                if (combined_map.find(pair.first) == combined_map.end())
                {
                    combined_map[pair.first] = pair.second;
                }
                else
                {
                    combined_map[pair.first] += pair.second;
                }
            }
        }

        // sort the map based on highest values
        vector<pair<string, int>> vec;
        for (const auto &it : combined_map)
        {
            vec.push_back(it);
        }
        sort(vec.begin(), vec.end(), [](const pair<string, int> &a, const pair<string, int> &b)
             { return a.second > b.second; });

        // print to file
        ofstream myfile;
        myfile.open("output_test.txt");
        for (const auto &it : vec)
        {
            // cout << it.first << " " << it.second << endl;
            myfile << it.first << " " << it.second << endl;
        }
    }
}

int main(int argc, char **argv)
{
    int rank, comm_size, num_files, max_length, min_length, words_length = 0;
    string file_name, str, words="";
    int *checkpoints;
    double start_time, end_time;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    if (!rank)
    {
        cout << "Enter the number of text files: ";
        cin >> num_files;
        int temp_words_length;
        for (int i = 0; i < num_files; i++)
        {
            printf("Enter the path of text file %d: ", i + 1);
            cin >> file_name;
            ifstream file(file_name);
            if (file)
            {
                ostringstream ss;
                ss << file.rdbuf(); // reading data
                str = ss.str();
                file.close();
                temp_words_length = str.size();
                words += str;
                words_length += temp_words_length;
            }
            else
            {
                cout << "File not found" << endl;
            }
        }
        cout << "Enter the minimum length of words to consider: ";
        cin >> min_length;
        cout << "Enter the maximum length of words to consider: ";
        cin >> max_length;

        // get checkpoint for paragraph eg. [0, 100, 200, 300, 400]
        checkpoints = get_checkpoints(comm_size, words, words_length);
    }
    start_time = MPI_Wtime();
    MPI_Bcast(&min_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&max_length, 1, MPI_INT, 0, MPI_COMM_WORLD);

    process_stream(comm_size, words, words_length, checkpoints, rank, min_length, max_length);

    // free(words);
    // free(checkpoints);
    // free(*substring);
    end_time = MPI_Wtime();
    if (!rank)
    {
        cout << "Time taken: " << end_time - start_time << endl;
    }
    MPI_Finalize();
}