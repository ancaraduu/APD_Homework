#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <ctype.h>

typedef struct {
    char word[50];
    int file_id;
} WordLocation;

typedef struct {
    char word[50];
    int *files_ids;
    int files_ids_len;
} WordMultipleLocation;

// structure that should hold an array for the words discovered by each thread
typedef struct {
    // [{miau, 1},{ham,2}], [{omege,3}]
    WordLocation **word_location_arrays;
    int *arrays_lens;
    int mapper_no;
} AllWordLocations;

typedef struct {
    // there are 26 arrays -> for each english alphabet char
    WordMultipleLocation **word_multiple_location_arrays;
    int *arrays_lens;
} WordsInFilesByInitial;

typedef struct {
    int *files_no;
    FILE **input_files;
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
    int thread_id;
    AllWordLocations *all_word_locations;
} MapperThreadPack;

typedef struct {
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
    pthread_barrier_t *barrier_before_sort;
    int thread_id;
    int *char_idx;
    AllWordLocations *all_word_locations;
    WordsInFilesByInitial* words_in_files_by_init;
} ReducerThreadPack;

// Function to remove punctuation from the beginning and end of a word
void cleanWord(char *word) {
    int start = 0;
    int end = strlen(word) - 1;

    // Remove punctuation from the beginning
    while (start <= end && ispunct(word[start])) {
        start++;
    }

    // Remove punctuation from the end
    while (end >= start && ispunct(word[end])) {
        word[end] = '\0';
        end--;
    }

    // Shift the cleaned word to the beginning of the string
    if (start > 0) {
        memmove(word, word + start, end - start + 2); // +2 to include null-terminator
    }
}

// Function to convert a string to lowercase
void toLowerCase(char* str) {
    for (int i = 0; str[i]; i++) {
        str[i] = tolower(str[i]);
    }
}

// Thread function
void* mapper_job(void* mapper_thread_pack_void) {
    
    MapperThreadPack *mapper_thread_pack = (MapperThreadPack *)mapper_thread_pack_void;  // Retrieve the size of the array from argument
    
    int *files_no = mapper_thread_pack->files_no;
    FILE **input_files = mapper_thread_pack->input_files;
    pthread_mutex_t *mutex = mapper_thread_pack->mutex;
    pthread_barrier_t *barrier = mapper_thread_pack->barrier;
    int thread_id = mapper_thread_pack->thread_id;
    AllWordLocations *all_word_locations = mapper_thread_pack->all_word_locations;

    printf("Hello from thread %d\n", thread_id);

    // Allocate memory for an array of WordLocation structures
    WordLocation* word_location_array = (WordLocation*)malloc(100 * sizeof(WordLocation));
    if (word_location_array == NULL) {
        perror("Failed to allocate memory");
        pthread_exit(NULL);  // Exit thread on allocation failure
    }

    all_word_locations->word_location_arrays[thread_id] = word_location_array;

    FILE *current_file;
    int current_file_id;

   int wordCount = 0;
    while(*files_no > 0) {
        // Get the next file
        pthread_mutex_lock(mutex);

        current_file = input_files[*files_no - 1];
        current_file_id = *files_no;
        *files_no = *files_no - 1;
 
        printf("Reading from file %d\n", current_file_id);

        pthread_mutex_unlock(mutex);

        // Populate the array
        // Parse words from the file
        char buffer[50]; 
        while (fscanf(current_file, "%50s", buffer) == 1) { // Read up to 99 characters into buffer
            // Clean the word by removing punctuation
            cleanWord(buffer);

            // Convert word to lowercase
            toLowerCase(buffer);

            printf("%s\n", buffer);

            int should_add = 1;
    




















































































































































































































































































    
    pthread_barrier_wait(barrier);

    // Return the pointer to the allocated array
    return NULL;
}

// Comparison function for qsort
int compareInts(const void *a, const void *b) {
    return (*(int *)a - *(int *)b); // Ascending order
}

// Comparison function for qsort
int compareWordMultipleLocation(const void *a, const void *b) {
    WordMultipleLocation *w1 = (WordMultipleLocation *)a;
    WordMultipleLocation *w2 = (WordMultipleLocation *)b;

    if (w1->files_ids_len > 1)
        qsort(w1->files_ids, w1->files_ids_len, sizeof(int), compareInts);

    if (w2->files_ids_len > 1)
        qsort(w2->files_ids, w2->files_ids_len, sizeof(int), compareInts);

    // First criterion: Compare files_ids_len
    if (w1->files_ids_len != w2->files_ids_len) {
        return w2->files_ids_len - w1->files_ids_len; // Ascending order
    }

    // Second criterion: Compare words alphabetically
    return strcmp(w1->word, w2->word); // Ascending lexicographical order
}

// Reducer Thread function
void* reducer_job(void* reducer_thread_pack_void) {
    
    ReducerThreadPack *reducer_thread_pack = (ReducerThreadPack *)reducer_thread_pack_void;  // Retrieve the size of the array from argument
    
    pthread_mutex_t *mutex = reducer_thread_pack->mutex;
    pthread_barrier_t *barrier = reducer_thread_pack->barrier;
    pthread_barrier_t *barrier_before_sort = reducer_thread_pack->barrier_before_sort;
    int thread_id = reducer_thread_pack->thread_id;
    int *char_idx = reducer_thread_pack->char_idx;
    AllWordLocations *all_word_locations = reducer_thread_pack->all_word_locations;
    WordsInFilesByInitial* words_in_files_by_init = reducer_thread_pack->words_in_files_by_init;

    pthread_barrier_wait(barrier);    

    printf("Hello from thread %d\n", thread_id);

    WordLocation *current_words_array;
    int current_len;
    while(all_word_locations->mapper_no > 0) {
        pthread_mutex_lock(mutex);

        current_words_array = all_word_locations->word_location_arrays[all_word_locations->mapper_no - 1];
        current_len = all_word_locations->arrays_lens[all_word_locations->mapper_no - 1];
        all_word_locations->mapper_no--;

        pthread_mutex_unlock(mutex);

        for (int i = 0; i < current_len; i++) {
            
            char word[50];
            strcpy(word, current_words_array[i].word);
            int current_file_id = current_words_array[i].file_id;

            printf("current word: %s\n", word);

            int char_idx = word[0] - 'a';

            int added = 0;
            for (int j = 0; j < words_in_files_by_init->arrays_lens[char_idx]; j++) {
                if (strcmp(words_in_files_by_init->word_multiple_location_arrays[char_idx][j].word, word) == 0) {
                    words_in_files_by_init->word_multiple_location_arrays[char_idx][j].files_ids[words_in_files_by_init->word_multiple_location_arrays[char_idx][j].files_ids_len] = current_file_id;
                    words_in_files_by_init->word_multiple_location_arrays[char_idx][j].files_ids_len++;
                    added = 1;
                    break;
                }
            }

            if (added == 0) {
                strcpy(words_in_files_by_init->word_multiple_location_arrays[char_idx][words_in_files_by_init->arrays_lens[char_idx]].word, word);
                words_in_files_by_init->word_multiple_location_arrays[char_idx][words_in_files_by_init->arrays_lens[char_idx]].files_ids = (int *)malloc(sizeof(int));
                words_in_files_by_init->word_multiple_location_arrays[char_idx][words_in_files_by_init->arrays_lens[char_idx]].files_ids[0] = current_file_id;
                words_in_files_by_init->word_multiple_location_arrays[char_idx][words_in_files_by_init->arrays_lens[char_idx]].files_ids_len++;
                words_in_files_by_init->arrays_lens[char_idx]++;
            }
        }
    }
    
    pthread_barrier_wait(barrier_before_sort);
    // Now sort

    WordMultipleLocation *current_word_multiple_location_arrays;
    int current_array_len;
    while(*char_idx < 26) {
        pthread_mutex_lock(mutex);

        current_word_multiple_location_arrays = words_in_files_by_init->word_multiple_location_arrays[*char_idx];
        current_array_len = words_in_files_by_init->arrays_lens[*char_idx];
        *char_idx = *char_idx + 1;

        pthread_mutex_unlock(mutex);

        qsort(current_word_multiple_location_arrays, current_array_len, sizeof(WordMultipleLocation), compareWordMultipleLocation);

        // Now write in the file
        char file_name[50];
        sprintf(file_name, "%c.txt", 'a' + *char_idx - 1);
        FILE *file = fopen(file_name, "w");

        for (int i = 0; i < current_array_len; i++) {
            fprintf(file, "%s : [", current_word_multiple_location_arrays[i].word);
            for(int j = 0; j < current_word_multiple_location_arrays[i].files_ids_len - 1; j++)
                fprintf(file, "%d ", current_word_multiple_location_arrays[i].files_ids[j]);
            fprintf(file, "%d]\n", current_word_multiple_location_arrays[i].files_ids[current_word_multiple_location_arrays[i].files_ids_len - 1]);
        }

        // Close the file
        fclose(file);
    }

    printf("Finished my job! (Im thread %d)\n", thread_id);

    // Return the pointer to the allocated array
    return NULL;
}

int main(int argc, char **argv)
{
    // Get the input arguments

    int mapper_no = atoi(argv[1]);
    int reducer_no = atoi(argv[2]);
    char input_file_name[100];
    strcpy(input_file_name, argv[3]);

    printf("mappers: %d, reducers: %d, input: %s\n", mapper_no, reducer_no, input_file_name);

    // Parse the input file

    FILE *input_file = fopen(input_file_name, "r");

    if (input_file == NULL) {
        perror("Error opening file");
        return EXIT_FAILURE;
    }

    int files_no; // Number of file names
    fscanf(input_file, "%d", &files_no);

    printf("--> Number of files: %d\n", files_no);

    char file_names[1000][1000]; // Names of all the input files
    for (int i = 0; i < files_no; i++) {
        strcpy(file_names[i], "../checker/");

        char rest_of_file_name[50];
        fscanf(input_file, "%s", rest_of_file_name);
        strcat(file_names[i], rest_of_file_name);

        printf("File name %d: %s\n", i + 1, file_names[i]);
    }

    fclose(input_file);

    // Open all input files
    FILE *input_files[files_no + 1];
    for (int i = 0; i < files_no; i++) {
        input_files[i] = fopen(file_names[i], "r");

        if (input_files[i] == NULL) {
            printf("Error opening file %s\n", file_names[i]);
            return EXIT_FAILURE;
        }
    }

    pthread_mutex_t *mutex = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(mutex, NULL);

    pthread_barrier_t *barrier = malloc(sizeof(pthread_barrier_t));
    if (pthread_barrier_init(barrier, NULL, mapper_no + reducer_no) != 0) {
        perror("Failed to initialize barrier");
        return 1;
    }

    pthread_barrier_t *barrier_before_sort = malloc(sizeof(pthread_barrier_t));
    if (pthread_barrier_init(barrier_before_sort, NULL, reducer_no) != 0) {
        perror("Failed to initialize barrier");
        return 1;
    }

    // Create the threads
    pthread_t threads[mapper_no + reducer_no + 1];
    int thread_ids[mapper_no + reducer_no + 1];

    AllWordLocations* all_word_locations = (AllWordLocations *)malloc(sizeof(AllWordLocations));
    all_word_locations->word_location_arrays = (WordLocation**)malloc(mapper_no * sizeof(WordLocation*));
    all_word_locations->arrays_lens = (int*)malloc(mapper_no * sizeof(int));
    all_word_locations->mapper_no = mapper_no;

    WordsInFilesByInitial* words_in_files_by_init = (WordsInFilesByInitial *)malloc(sizeof(WordsInFilesByInitial));
    words_in_files_by_init->word_multiple_location_arrays = (WordMultipleLocation**)malloc(26 * sizeof(WordMultipleLocation*));
    words_in_files_by_init->arrays_lens = (int*)malloc(26 * sizeof(int));

    for (int i = 0; i < 26; i++) {
        words_in_files_by_init->word_multiple_location_arrays[i] = (WordMultipleLocation*)malloc(26 * sizeof(WordMultipleLocation));
        words_in_files_by_init->arrays_lens[i] = 0;
    }

    // Array that will hold the output files;
    // FILE *output_files[26];

    // Create the first half of threads
    for (int i = 0; i < mapper_no + reducer_no; i++) {
        thread_ids[i] = i + 1;

        if (i < mapper_no) {
            printf("Creating mapper thread %d\n", i);

            MapperThreadPack* mapper_thread_pack = (MapperThreadPack*)malloc(sizeof(MapperThreadPack));
            mapper_thread_pack->files_no = &files_no;
            mapper_thread_pack->input_files = input_files;
            mapper_thread_pack->mutex = mutex;
            mapper_thread_pack->barrier = barrier;
            mapper_thread_pack->thread_id = i;
            mapper_thread_pack->all_word_locations = all_word_locations;

            if (pthread_create(&threads[i], NULL, mapper_job, (void*)mapper_thread_pack) != 0) {
                perror("Error creating thread");
                return EXIT_FAILURE;
            }
        } else {
            printf("Creating reducer thread %d\n", i);

            ReducerThreadPack* reducer_thread_pack = (ReducerThreadPack*)malloc(sizeof(ReducerThreadPack));
            reducer_thread_pack->mutex = mutex;
            reducer_thread_pack->barrier = barrier;
            reducer_thread_pack->barrier_before_sort = barrier_before_sort;
            reducer_thread_pack->thread_id = i - mapper_no;
            int char_idx = 0;
            reducer_thread_pack->char_idx = &char_idx;
            reducer_thread_pack->all_word_locations = all_word_locations;
            reducer_thread_pack->words_in_files_by_init = words_in_files_by_init;

            if (pthread_create(&threads[i], NULL, reducer_job, (void*)reducer_thread_pack) != 0) {
                perror("Error creating thread");
                return EXIT_FAILURE;
            }
        }
    }
    

    // Wait for all threads to finish
    for (int i = 0; i < mapper_no + reducer_no; i++) {
        pthread_join(threads[i], NULL);
    }

    printf("Joined all\n");

    // for (int i = 0; i < mapper_no; i++) {
    //     for (int j = 0; j < all_word_locations->arrays_lens[i]; j++)
    //         printf("{%s, %d}, ", all_word_locations->word_location_arrays[i][j].word, all_word_locations->word_location_arrays[i][j].file_id);
    //     printf("\n");
    // }

    // for (int i = 0; i < 26; i++) {
    //     printf("%c:\n", 'a' + i);

    //     for(int j = 0; j < words_in_files_by_init->arrays_lens[i]; j++) {
    //         printf("%s -> ", words_in_files_by_init->word_multiple_location_arrays[i][j].word);
    //         for (int t = 0; t < words_in_files_by_init->word_multiple_location_arrays[i][j].files_ids_len; t++)
    //             printf("%d, ", words_in_files_by_init->word_multiple_location_arrays[i][j].files_ids[t]);
    //         printf("\n");
    //     }
    // }

    // Close all input files
    for (int i = 0; i < files_no; i++) {
        fclose(input_files[i]);
    }

    // Destroy mutex
    pthread_mutex_destroy(mutex);

    // Destroy the barrier
    pthread_barrier_destroy(barrier);

    return 0;
}