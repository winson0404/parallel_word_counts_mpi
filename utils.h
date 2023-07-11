#include <unordered_map>
#include <fstream>
#include <string>
#include <vector>
#include <iostream>

struct serialized_map{
    int total_words_length;
    int total_char_length;
    int* word_length;
    int* word_freq;
    std::string words;
};

int is_alphabet(char c)
{
    return (c >= 65 && c <= 90) || (c >= 97 && c <= 122);
}


//algorithm for splitting the whole paragraph across all the processes
int *get_checkpoints(int comm_size, std::string words, int length)
{
    int *_checkpoints = (int *)malloc(sizeof(int) * (comm_size + 1));
    int step = length / comm_size;
    int ptr = 0, counter = 0;

    // assign and make sure the first pointer is pointing the start of a word
    while (!is_alphabet(words[ptr]))
    {
        ptr++;
    }
    _checkpoints[0] = ptr;
    
    // assign the last pointer to the end 
    ptr = length - 1;
    while (!is_alphabet(words[ptr]))
    {
        ptr--;
    }
    _checkpoints[comm_size] = ptr;

    // get all the checkpoints in the middle
    ptr = step;
    while (counter < comm_size - 1)
    {
        // get the rightmost non alphabet character
        while (is_alphabet(words[ptr]))
        {
            ptr++;
        }
        _checkpoints[++counter] = ptr;
        ptr += step;
    }

    return _checkpoints;
}

std::unordered_map<std::string, int> get_words(std::string paragraph, int length, int min_length, int max_length)
{
    std::unordered_map<std::string, int> output;
    int ptr1 = 0, ptr2;
    while (ptr1 < length)
    {
        if(!is_alphabet(paragraph[ptr1])){
            ptr1++;
            continue;
        }
        paragraph[ptr1] = tolower(paragraph[ptr1]);
        ptr2 = ptr1 + 1;
        while (1)
        {
            if (!is_alphabet(paragraph[ptr2]))
                break;
            paragraph[ptr2] = tolower(paragraph[ptr2]);
            ptr2++;
        }
        int word_length = ptr2 - ptr1;
        if (word_length < min_length || word_length > max_length)
        {
            ptr1 = ptr2;
            continue;
        } 
        if (output.find(paragraph.substr(ptr1, word_length)) == output.end())
        {
            output[paragraph.substr(ptr1, word_length)] = 1;
        }
        else
        {
            output[paragraph.substr(ptr1, word_length)]++;
        }

        // free(word);

        ptr1 = ptr2;
    }

    return output;
}



serialized_map serialize_map(std::unordered_map<std::string, int> map, int rank)
{

    serialized_map output;
    output.total_words_length = map.size();
    output.words = "";
    output.total_char_length = 0;
    output.word_length = (int*) malloc(sizeof(int)* output.total_words_length);
    output.word_freq  = (int*) malloc(sizeof(int)* output.total_words_length);
    int i = 0, char_length = 0, temp_char_length = 0;
    for (const auto &pair : map)
    {
        output.words+=pair.first;
        output.word_length[i] = pair.first.length();
        output.total_char_length+=output.word_length[i];
        output.word_freq[i] = pair.second;
        temp_char_length += output.word_length[i];

        i++;
    }



    return output;
}

std::unordered_map<std::string, int> deserialize_map(serialized_map in)
{
    std::unordered_map<std::string, int> output;
    int ptr1 = 0;
    std::string word;
    // std::cout << in.words << std::endl;
    for(int i = 0; i < in.total_words_length; i++){
        // strncpy(word, in.words + ptr1, in.word_length[i]);
        // std::cout << in.word_length[i] << std::endl;
        output[in.words.substr(ptr1, in.word_length[i])] = in.word_freq[i];
        ptr1+=in.word_length[i];
        // free(word);
    }
    return output;
}