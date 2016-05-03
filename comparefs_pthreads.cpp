#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <math.h>

#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <pthread.h>
#include <semaphore.h>

#include <openssl/md5.h>

#include <queue>
#include <string>
#include <map>

#define BUFFER_SIZE 4096

using namespace std;

struct Buffer {
    void* buff;
    off_t size;
    bool is_file;
    bool is_finished;

    Buffer() : size(0), is_file(true), is_finished(false) { buff = malloc(BUFFER_SIZE); }
    ~Buffer() { free(buff); }
};

struct Parameters {
    string path;
    int t_id;
};

Buffer *buffer;
sem_t *fs_threads;
sem_t control_thread;
bool next_child = true;
bool next_queue = true;
bool exit_thread = false;
int current_field = 0;
int num_threads;
int response_count;
pthread_mutex_t response_count_mutex = PTHREAD_MUTEX_INITIALIZER;
int thread_majority;


struct MajorityFolder
{
    string rel_path;
    bool *mask;
    bool is_common;
    int folder_count;
    int majority_count;

    MajorityFolder() { mask = new bool[num_threads]; }
    ~MajorityFolder() { delete [] mask; }
};

queue<MajorityFolder*> bfs_queue;


// logical or of two bool arrays
void and_boolarrays(bool* parent, bool* child)
{
    int count = 0;
    for (int i=0; i < num_threads; i++)
    {
        if (child[i] == true)
            count ++;
    }

    if (count > thread_majority) {
        for (int i=0; i < num_threads; i++)
        {
            parent[i] = parent[i]&child[i];
        }
    }
}

int reset_compare(int* equal_index, int* count_equal)
{
    int num_finished_threads = 0;
    for (int i = 0; i < num_threads; i ++)
    {
        if (buffer[i].is_finished)
        {
            equal_index[i] = -1;
            count_equal[i] = -1;
            num_finished_threads ++;
        }
        else
        {
            equal_index[i] = i;
            count_equal[i] = 1;
        }
    }

    return num_finished_threads;
}

inline void wake_threads()
{
    for (int i = 0; i < num_threads; i ++)
    {
        sem_post(&fs_threads[i]);
    }
}

inline int wake_threads_by_condition(bool* condition_arr, bool value, int start_index)
{
    int thread_satisfies = 0;
    int i;

    for (i = start_index; i < num_threads; i ++)
    {
        if (condition_arr[i] == value) thread_satisfies ++;
    }
    response_count -= thread_satisfies;
    for (i = start_index; i < num_threads; i ++)
    {
        if (condition_arr[i] == value)
            sem_post(&fs_threads[i]);
    }

    return thread_satisfies;
}

inline int wake_threads_by_condition(int* condition_arr, int value, int start_index)
{
    int thread_satisfies = 0;
    int i;

    for (i = start_index; i < num_threads; i ++)
    {
        if (condition_arr[i] == value)
            thread_satisfies ++;
    }

    response_count -= thread_satisfies;

    for (i = start_index; i < num_threads; i ++)
    {
        if (condition_arr[i] == value)
            sem_post(&fs_threads[i]);
    }

    return thread_satisfies;
}

int compare_pathnames(int* equal_index, int* count_equal)
{
    int min_index = -1;
    int compare_result;

    for (int i = 0; i < num_threads; i ++)
    {
        if (equal_index[i] < i)
            continue;
        else
        {
            if (min_index == -1)
            {
                min_index = i;
            }
            else
            {
                if (strcmp((char*)buffer[min_index].buff, (char*)buffer[i].buff) > 0)
                {
                    min_index = i;
                }
                else if (strcmp((char*)buffer[min_index].buff, (char*)buffer[i].buff) == 0 &&
                        buffer[min_index].is_file == false &&
                        buffer[i].is_file == true)
                {
                    min_index = i;
                }

            }

            for (int j = i + 1; j < num_threads; j ++) {
                if (equal_index[j] < j)
                    continue;

                compare_result = strcmp((char*)buffer[i].buff, (char*)buffer[j].buff);
                if (compare_result == 0 && buffer[i].is_file == buffer[j].is_file) {
                    equal_index[j] = equal_index[i];
                    count_equal[i] ++;
                }
            }
        }
    }

    return min_index;
}

void compare_sizes(int* equal_index, int* count_equal)
{
    for (int i = 0; i < num_threads; i ++)
    {
        if (equal_index[i] < i)
            continue;
        else
        {
            for (int j = i + 1; j < num_threads; j ++)
            {
                if (equal_index[j] < j)
                    continue;
                if (memcmp(buffer[i].buff, buffer[j].buff, sizeof(off_t))==0)
                {
                    equal_index[j] = equal_index[i];
                    count_equal[i] ++;
                }
            }
        }
    }
}

void compare_filecontent(int* equal_index, int* count_equal)
{
    for (int i = 0; i < num_threads; i ++)
    {
        if (equal_index[i] < i)
            continue;
        else
        {
            for (int j = i + 1; j < num_threads; j ++)
            {
                if (equal_index[j] < j)
                    continue;
                if (memcmp(buffer[i].buff, buffer[j].buff, buffer[i].size)==0)
                {
                    equal_index[j] = equal_index[i];
                    count_equal[i] ++;
                }
            }
        }
    }
}

int find_max_index(int* count_equal)
{
    int max_count = 0;
    int max_index = -1;

    for (int i = 0; i < num_threads; i ++)
    {
        if (count_equal[i] > max_count)
        {
            max_count = count_equal[i];
            max_index = i;
        }
    }

    return max_index;
}

void remove_all_nonmin(int* equal_index, int* count_equal, int min_index)
{
    for (int i = 0; i < num_threads; i ++)
    {
        if (equal_index[i] == -1 || equal_index[i]!=min_index)
        {
            equal_index[i] = -1;
            count_equal[i] = -1;
        }
        else
        {
            equal_index[i] = i;
            count_equal[i] = 1;
        }
    }
}

void compare_hash(int* equal_index, int* count_equal)
{
    for (int i = 0; i < num_threads; i ++)
    {
        if (equal_index[i] < i)
            continue;
        else
        {
            for (int j = i + 1; j < num_threads; j ++)
            {
                if (equal_index[j] < j)
                    continue;
                if (memcmp(buffer[i].buff, buffer[j].buff, MD5_DIGEST_LENGTH)==0)
                {
                    equal_index[j] = equal_index[i];
                    count_equal[i] ++;
                }
            }
        }
    }
}

void compare_children(int* equal_index, int* count_equal)
{
    for (int i = 0; i < num_threads; i ++)
    {
        if (equal_index[i] < i)
            continue;
        else
        {
            for (int j = i + 1; j < num_threads; j ++)
            {
                if (equal_index[j] < j)
                    continue;
                if (memcmp(buffer[i].buff, buffer[j].buff, sizeof(int))==0)
                {
                    equal_index[j] = equal_index[i];
                    count_equal[i] ++;
                }
            }
        }
    }
}

MajorityFolder* getParentFolder(string child_folder, map<string, MajorityFolder*> &parent_store) {
    auto smallstr = child_folder.substr(0, child_folder.size()-1);
    size_t found = smallstr.rfind("/");
    if(found != string::npos) {
        return parent_store[child_folder.substr(0, found+1)];
    }
    else
        return NULL;
}

bool handle_empty_threads(int num_empty_threads, MajorityFolder* &current_mf, map<string, MajorityFolder*> &parent_store)
{
    int smf_count;
    MajorityFolder* parent_mf;

    if (num_empty_threads > thread_majority)
    {

        if (bfs_queue.empty())
        {
            exit_thread = true;
            response_count = 0;
            wake_threads();
        }
        else
        {

            if (next_queue)
            {
                while (current_mf != NULL && (current_mf->folder_count == 0)) {
                    if (current_mf->folder_count == 0) {
                        parent_mf = getParentFolder(current_mf->rel_path, parent_store);

                        // print_if_majority_folder or common folder
                        smf_count = 0;
                        for (int i = 0; i < num_threads; i ++) {
                            if (current_mf->mask[i] == 1)
                                smf_count ++;
                        }

                        if (smf_count > thread_majority && current_mf->majority_count>0) {
                            if (smf_count == num_threads && current_mf->is_common==1) {
                                printf("%s is a common folder.\n", current_mf->rel_path.c_str());
                            }
                            else {
                                printf("%s is a majority folder. File Systems : ", current_mf->rel_path.c_str());
                                for (int i = 0; i < num_threads; i ++)
                                    if (current_mf->mask[i] == 1)
                                        printf(" %d ", i);
                                    printf("\n");
                            }
                            if (parent_mf != NULL) {
                                and_boolarrays(parent_mf->mask, current_mf->mask);
                                parent_mf->majority_count += 1;
                            }
                        }
                        if (parent_mf != NULL) {
                            parent_mf->folder_count -= 1;
                            parent_mf->is_common = parent_mf->is_common & current_mf->is_common;
                        }

                        // //delete folder_count zero cases from hashtable and memory.
                        parent_store.erase(current_mf->rel_path);
                        delete current_mf;

                        // repeatedly update parent chain until folder_count not zero.
                        if (parent_mf!=NULL && parent_mf->folder_count == 0) {
                            current_mf = parent_mf;
                        } else {
                            current_mf = NULL;
                        }

                    }
                }

                current_mf = bfs_queue.front();
                current_mf->majority_count += 1;

                bfs_queue.pop();

                while (current_mf != NULL && (current_mf->folder_count == 0)) {
                    if (current_mf->folder_count == 0) {
                        parent_mf = getParentFolder(current_mf->rel_path, parent_store);

                        // print_if_majority_folder or common folder
                        smf_count = 0;
                        for (int i = 0; i < num_threads; i ++) {
                            if (current_mf->mask[i] == 1)
                                smf_count ++;
                        }

                        if (smf_count > thread_majority && current_mf->majority_count>0) {
                            if (smf_count == num_threads && current_mf->is_common==1) {
                                printf("%s is a common folder.\n", current_mf->rel_path.c_str());
                            }
                            else {
                                printf("%s is a majority folder. File Systems : ", current_mf->rel_path.c_str());
                                for (int i = 0; i < num_threads; i ++)
                                    if (current_mf->mask[i] == 1)
                                        printf(" %d ", i);
                                    printf("\n");
                            }
                            if (parent_mf != NULL) {
                                and_boolarrays(parent_mf->mask, current_mf->mask);
                                parent_mf->majority_count += 1;
                            }
                        }
                        if (parent_mf != NULL) {
                            parent_mf->folder_count -= 1;
                            parent_mf->is_common = parent_mf->is_common & current_mf->is_common;
                        }

                        // //delete folder_count zero cases from hashtable and memory.
                        parent_store.erase(current_mf->rel_path);
                        delete current_mf;

                        // repeatedly update parent chain until folder_count not zero.
                        if (parent_mf!=NULL && parent_mf->folder_count == 0) {
                            current_mf = parent_mf;
                        } else {
                            current_mf = NULL;
                        }

                    }
                }
            }

            if (bfs_queue.empty()) {
                exit_thread = 1;
                response_count = 0;
                wake_threads();
            } else {
                next_child = true;
                next_queue = true;
                current_field = 0;

                /*for (int i = 0; i < num_threads; i ++)
                    if (bfs_queue.front()->mask[i] !=true)
                        buffer[i].is_finished = true;
                */
                wake_threads_by_condition(bfs_queue.front()->mask, true, 0);
            }

        }
        return true;

    }
    else
    {
        next_child = false;
        return false;
    }
}

// checks if b is a prefix of a.
bool starts_with(string a, string b)
{
    return (strncmp(a.c_str(), b.c_str(), b.length()) == 0);
}

void print_if_majority_folder(MajorityFolder* mf)
{
    int count = 0;
    for (int i = 0; i < num_threads; i++)
    {
        if (mf->mask[i])
            count ++;
    }

    if (count > thread_majority)
    {
        if (count == thread_majority)
            printf("%s is a common folder.\n", mf->rel_path.c_str());
        else {
            printf("%s is a majority folder. File Systems : ", mf->rel_path.c_str());
            for (int i = 0; i < num_threads; i++)
                if (mf->mask[i])
                    printf(" %d ",i);
            printf("\n");
        }
    }
}

void update_majority_info(MajorityFolder* mf, int* equal_index, int max_equal_index)
{
    for (int i = 0; i < num_threads; i++)
    {
        mf->mask[i] = mf->mask[i] * (equal_index[i] == max_equal_index);
    }
}

void control_threads()
{
    map<string, MajorityFolder*> parent_store;
    int num_empty_threads, min_index, max_equal_index;
    int *equal_index, *equal_index_copy, *count_equal;
    MajorityFolder* current_mf = NULL;
    MajorityFolder* parent_mf;
    int smf_count;

    string current_dir;

    string possible_majority_pathname;
    off_t possible_majority_filesize = 0;
    bool possible_majority_path_is_file = true;

    equal_index = new int[num_threads];
    equal_index_copy = new int[num_threads];
    count_equal = new int[num_threads];

    while (1)
    {
        sem_wait(&control_thread);

        if (exit_thread)
        {
            while (current_mf != NULL) {
                if (current_mf->folder_count == 0) {
                    parent_mf = getParentFolder(current_mf->rel_path, parent_store);

                    // print_if_majority_folder or common folder
                    smf_count = 0;
                    for (int i = 0; i < num_threads; i ++) {
                        if (current_mf->mask[i] == 1)
                            smf_count ++;
                    }

                    if (smf_count > thread_majority && current_mf->majority_count>0) {
                        if (smf_count == num_threads && current_mf->is_common==1) {
                            printf("%s is a common folder.\n", current_mf->rel_path.c_str());
                        }
                        else {
                            printf("%s is a majority folder. File Systems : ", current_mf->rel_path.c_str());
                            for (int i = 0; i < num_threads; i ++)
                                if (current_mf->mask[i] == 1)
                                    printf(" %d ", i);
                                printf("\n");
                        }
                        if (parent_mf != NULL) {
                            and_boolarrays(parent_mf->mask, current_mf->mask);
                            parent_mf->majority_count += 1;
                        }
                    }
                    if (parent_mf != NULL) {
                        parent_mf->folder_count -= 1;
                        parent_mf->is_common = parent_mf->is_common & current_mf->is_common;
                    }

                    // //delete folder_count zero cases from hashtable and memory.
                    parent_store.erase(current_mf->rel_path);
                    delete current_mf;

                    // repeatedly update parent chain until folder_count not zero.

                    if (parent_mf!=NULL && parent_mf->folder_count == 0) {
                        current_mf = parent_mf;
                    } else {
                        current_mf = NULL;
                    }
                }
            }

            delete [] count_equal;
            delete [] equal_index;
            delete [] equal_index_copy;
            return;
        }

        if (next_child)
        {
            num_empty_threads = reset_compare(equal_index, count_equal);
            if (handle_empty_threads(num_empty_threads, current_mf, parent_store))
                continue;
        }

        if (next_queue)
        {
            while (current_mf != NULL && (current_mf->folder_count == 0)) {
                if (current_mf->folder_count == 0) {
                    parent_mf = getParentFolder(current_mf->rel_path, parent_store);

                    // print_if_majority_folder or common folder
                    smf_count = 0;
                    for (int i = 0; i < num_threads; i ++) {
                        if (current_mf->mask[i] == 1)
                            smf_count ++;
                    }

                    if (smf_count > thread_majority && current_mf->majority_count>0) {
                        if (smf_count == num_threads && current_mf->is_common==1) {
                            printf("%s is a common folder.\n", current_mf->rel_path.c_str());
                        }
                        else {
                            printf("%s is a majority folder. File Systems : ", current_mf->rel_path.c_str());
                            for (int i = 0; i < num_threads; i ++)
                                if (current_mf->mask[i] == 1)
                                    printf(" %d ", i);
                                printf("\n");
                        }
                        if (parent_mf != NULL) {
                            and_boolarrays(parent_mf->mask, current_mf->mask);
                            parent_mf->majority_count += 1;
                        }
                    }
                    if (parent_mf != NULL) {
                        parent_mf->folder_count -= 1;
                        parent_mf->is_common = parent_mf->is_common & current_mf->is_common;
                    }

                    // delete folder_count zero cases from hashtable and memory.
                    parent_store.erase(current_mf->rel_path);
                    delete current_mf;

                    // repeatedly update parent chain until folder_count not zero.
                    if (parent_mf!=NULL && parent_mf->folder_count == 0) {
                        current_mf = parent_mf;
                    } else {
                        current_mf = NULL;
                    }
                }
            }

            current_mf = bfs_queue.front();
            current_dir = current_mf->rel_path;
            bfs_queue.pop();

            parent_store.insert({current_dir, current_mf});

            next_queue = 0;
        }

        // more efficient than an if-else ladder as it will be implemented as a jump table by the compiler.
        switch (current_field)
        {
            case 0:
            {
                min_index = compare_pathnames(equal_index, count_equal);
                max_equal_index = find_max_index(count_equal);

                if (max_equal_index == min_index && count_equal[max_equal_index] > thread_majority)
                {
                    possible_majority_pathname = (const char*) buffer[min_index].buff;
                    possible_majority_path_is_file = buffer[min_index].is_file;

                    if (!possible_majority_path_is_file)
                    {
                        current_mf->folder_count += 1;

                        MajorityFolder* new_maj_folder = new MajorityFolder();
                        new_maj_folder->rel_path = current_dir + possible_majority_pathname + "/";
                        new_maj_folder->is_common = 1;
                        new_maj_folder->folder_count = 0;
                        new_maj_folder->majority_count = 0;

                        for (int i = 0; i < num_threads; i ++)
                        {
                            if (equal_index[i] == min_index)
                                new_maj_folder->mask[i] = true;
                            else {
                                new_maj_folder->mask[i] = false;
                                new_maj_folder->is_common = 0;
                            }
                        }

                        bfs_queue.push(new_maj_folder);

                        current_field = 0;

                        next_child = true;
                        wake_threads_by_condition(equal_index, min_index, min_index);
                    }
                    else
                    {
                        current_field = 1;

                        memcpy(equal_index_copy, equal_index, sizeof(int)*num_threads);
                        remove_all_nonmin(equal_index, count_equal, min_index);

                        wake_threads_by_condition(equal_index_copy, min_index, min_index);
                    }
                }
                else
                {
                    current_mf->is_common = 0;

                    next_child = true;
                    wake_threads_by_condition(equal_index, min_index, min_index);
                }

                break;
            }

            case 1:
            {
                compare_sizes(equal_index, count_equal);
                max_equal_index = find_max_index(count_equal);

                if (count_equal[max_equal_index] > thread_majority)
                {
                    current_field = 2;
                    possible_majority_filesize = *((off_t*) buffer[max_equal_index].buff);
                    remove_all_nonmin(equal_index, count_equal, max_equal_index);

                    wake_threads_by_condition(count_equal, 1, max_equal_index);
                }
                else
                {
                    current_mf->is_common = 0;
                    next_child = true;
                    current_field = 0;
                    wake_threads_by_condition(equal_index_copy, min_index, min_index);
                }

                break;
            }

            case 2:
            {
                compare_hash(equal_index, count_equal);
                max_equal_index = find_max_index(count_equal);

                if (count_equal[max_equal_index] > thread_majority)
                {
                    current_field = 3;
                    remove_all_nonmin(equal_index, count_equal, max_equal_index);

                    wake_threads_by_condition(count_equal, 1, max_equal_index);
                }
                else
                {
                    current_mf->is_common = 0;
                    next_child = true;
                    current_field = 0;

                    wake_threads_by_condition(equal_index_copy, min_index, min_index);
                }

                break;
            }

            case 3:
            {
                compare_filecontent(equal_index, count_equal);
                max_equal_index = find_max_index(count_equal);

                if (possible_majority_filesize < BUFFER_SIZE)
                {
                    if (count_equal[max_equal_index] > thread_majority)
                    {
                        if (count_equal[max_equal_index] == num_threads)
                            printf("%s is a common file.\n", (current_dir + possible_majority_pathname).c_str());
                        else {
                            printf("%s is a majority file. File Systems : ", (current_dir + possible_majority_pathname).c_str());
                            for (int i = 0; i < num_threads; i ++)
                                if (equal_index[i] == max_equal_index)
                                    printf(" %d ", i);
                            printf("\n");
                        }
                        update_majority_info(current_mf, equal_index, max_equal_index);
                        current_mf->majority_count += 1;
                    }
                    else
                    {
                        current_mf->is_common = 0;
                    }

                    next_child = true;
                    current_field = 0;
                    possible_majority_filesize = 0;

                    wake_threads_by_condition(equal_index_copy, min_index, min_index);
                }
                else
                {
                    possible_majority_filesize -= BUFFER_SIZE;

                    if (count_equal[max_equal_index] > thread_majority)
                    {
                        remove_all_nonmin(equal_index, count_equal, max_equal_index);
                        wake_threads_by_condition(count_equal, 1, max_equal_index);
                    }
                    else
                    {
                        next_child = true;

                        current_mf->is_common = 0;
                        current_field = 0;
                        possible_majority_filesize = 0;

                        wake_threads_by_condition(equal_index_copy, min_index, min_index);
                    }
                }
            }
        }
    }
}

void increment_and_post() {
    pthread_mutex_lock(&response_count_mutex);
    response_count += 1;

    if (response_count == num_threads)
        sem_post(&control_thread);

    pthread_mutex_unlock(&response_count_mutex);
}

inline void cleanup_filelist(struct dirent** l, int num_child) {
    for (int i = 0; i < num_child; i ++)
        free(l[i]);

    free(l);
}

int dotfilter(const struct dirent* entry) {
    if (strcmp(entry->d_name, ".")==0)
        return 0;
    if (strcmp(entry->d_name, "..")==0)
        return 0;
    return 1;
}

void* filesystemwalk(void* arg) {
    Parameters *p = (Parameters*) arg;
    int num_child = 0;
    int current_child = 0;
    struct dirent** child_list = NULL;
    string current_dir_root;
    struct stat st_buff;
    int current_fd = -1;

    while(1) {
        sem_wait(&fs_threads[p->t_id]);

        if (exit_thread) {
            cleanup_filelist(child_list, num_child);
            increment_and_post();
            return NULL;
        }

        if (next_queue) {
            buffer[p->t_id].is_finished = false;
            cleanup_filelist(child_list, num_child);
            current_child = -1;
            current_dir_root = p->path + bfs_queue.front()->rel_path;
            num_child = scandir(current_dir_root.c_str(), &child_list, &dotfilter, alphasort);
        }

        if (next_child) {
            if (current_fd != -1) {
                close(current_fd);
                current_fd = -1;
            }
        /*    if (folder_num_child != 0) {
                cleanup_filelist(folder_child_list, folder_num_child);
                folder_num_child = 0;
            }*/
            current_child ++;
        }

        if (current_child == num_child) {
            buffer[p->t_id].is_finished = true;
            increment_and_post();
            continue;
        }

        if (current_field == 0) {
            strcpy((char*)buffer[p->t_id].buff, child_list[current_child]->d_name);
            stat((current_dir_root + child_list[current_child]->d_name).c_str(), &st_buff);
            buffer[p->t_id].is_file = S_ISREG(st_buff.st_mode);
            increment_and_post();
            continue;
        }

        if (current_field == 1) {
            memcpy(buffer[p->t_id].buff, &(st_buff.st_size), sizeof(off_t));
            buffer[p->t_id].size = sizeof(off_t);
            increment_and_post();
            continue;
        }

        if (current_field == 2) {
            current_fd = open((current_dir_root + child_list[current_child]->d_name).c_str(), O_RDONLY);

            MD5_CTX mdContext;
            unsigned char c[MD5_DIGEST_LENGTH];
            int bytes;

            MD5_Init(&mdContext);
            while((bytes = read(current_fd, buffer[p->t_id].buff, BUFFER_SIZE)) != 0) {
                MD5_Update(&mdContext, buffer[p->t_id].buff, bytes);
            }
            MD5_Final(c, &mdContext);
            memcpy(buffer[p->t_id].buff, c, MD5_DIGEST_LENGTH);
            buffer[p->t_id].size = MD5_DIGEST_LENGTH;
            lseek(current_fd, 0, SEEK_SET);
            increment_and_post();
            continue;
        }

        if (current_field == 3) {
            int bytes;
            bytes = read(current_fd, buffer[p->t_id].buff, BUFFER_SIZE);
            buffer[p->t_id].size = bytes;
            increment_and_post();
            continue;
        }
    }
    return NULL;
}

int main(int argc, char* argv[])
{
    int i;

    if (argc < 3)
    {
        printf("Atleast 2 folders are required for comparison\n");

        return 0;
    }

    num_threads = argc - 1;
    thread_majority = num_threads/2;
    pthread_t *fswalk = new pthread_t[num_threads];
    fs_threads = new sem_t[num_threads];
    Parameters *params = new Parameters[num_threads] ();
    buffer = new Buffer[num_threads] ();

    sem_init(&control_thread, 0, 0);

    MajorityFolder* q = new MajorityFolder;
    q->rel_path = "/";
    q->is_common = 1;
    q->folder_count = 0;
    q->majority_count = 0;

    for (i = 0; i < num_threads; i ++)
    {
        q->mask[i] = true;
    }
    bfs_queue.push(q);

    void* (*fn_ptr) (void*);
    fn_ptr =  &filesystemwalk;

    for (int i = 0; i < num_threads; i ++)
    {
        params[i].t_id = i;
        params[i].path = argv[i + 1];
        sem_init(&fs_threads[i], 0, 1);
        pthread_create(&fswalk[i], NULL, fn_ptr, &params[i]);
    }

    control_threads();

    for (i = 0; i < num_threads; i ++)
    {
        pthread_join(fswalk[i], NULL);
    }

    delete [] fswalk;
    delete [] fs_threads;
    delete [] params;
    delete [] buffer;

    return 0;
}
