#include <omp.h>
#include <iostream>
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

#include <openssl/md5.h>

#include <queue>
#include <string>
#include <map>

#define BUFFER_SIZE 4096

using namespace std;

struct Buffer {
    string base_path;
    void* buff;
    off_t size;
    bool is_file;
    bool is_finished;

    Buffer() : size(0), is_file(true), is_finished(false) { buff = malloc(BUFFER_SIZE); }
    ~Buffer() { free(buff); }
};

Buffer *buffer;
int thread_majority, n_threads;

struct MajorityFolder
{
    string rel_path;
    int *mask;
    int is_common;
    int folder_count;
    int majority_count;

    MajorityFolder() { mask = new int[n_threads]; }
    ~MajorityFolder() { delete [] mask; }
};

queue<MajorityFolder*> bfs_queue;

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

int find_max_index(int* count_equal) {
    int max_count = 0;
    int max_index = -1;

    for (int i = 0; i < n_threads; i ++) {
        if (count_equal[i] > max_count) {
            max_count = count_equal[i];
            max_index = i;
        }
    }

    return max_index;
}

void compare_filecontent(int* equal_index, int* count_equal)
{
    for (int i = 0; i < n_threads; i ++)
    {
        if (equal_index[i] < i)
            continue;
        else
        {
            for (int j = i + 1; j < n_threads; j ++)
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

int compare_pathnames(int* equal_index, int* count_equal)
{
    int min_index = -1;
    int compare_result;

    for (int i = 0; i < n_threads; i ++)
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

            for (int j = i + 1; j < n_threads; j ++) {
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
    for (int i = 0; i < n_threads; i ++)
    {
        if (equal_index[i] < i)
            continue;
        else
        {
            for (int j = i + 1; j < n_threads; j ++)
            {
                if (equal_index[j] < j)
                    continue;
                if (buffer[i].size == buffer[j].size)
                {
                    equal_index[j] = equal_index[i];
                    count_equal[i] ++;
                }
            }
        }
    }
}

bool starts_with(string a, string b) {
    return (strncmp(a.c_str(), b.c_str(), b.length()) == 0);
}

void update_majority_info(MajorityFolder* mf, int* equal_index, int max_equal_index)
{
    for (int i = 0; i < n_threads; i++) {
        mf->mask[i] = mf->mask[i] & (equal_index[i] == max_equal_index);
    }
}

void compare_hash(int* equal_index, int* count_equal) {
    for (int i = 0; i < n_threads; i ++) {
        if (equal_index[i] < i)
            continue;
        else {
            for (int j = i + 1; j < n_threads; j ++) {
                if (equal_index[j] < j)
                    continue;
                if (memcmp(buffer[i].buff, buffer[j].buff, MD5_DIGEST_LENGTH)==0) {
                    equal_index[j] = equal_index[i];
                    count_equal[i] ++;
                }
            }
        }
    }
}

void remove_all_nonmin(int* equal_index, int* count_equal, int min_index) {
    for (int i = 0; i < n_threads; i ++) {
        if (equal_index[i] == -1 || equal_index[i]!=min_index) {
            equal_index[i] = -1;
            count_equal[i] = -1;
        }
        else {
            equal_index[i] = i;
            count_equal[i] = 1;
        }
    }
}

int reset_compare(int* equal_index, int* count_equal)
{
    int num_finished_threads = 0;
    for (int i = 0; i < n_threads; i ++)
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

MajorityFolder* getParentFolder(map<string, MajorityFolder*> &parent_store,string child_folder) {
    auto smallstr = child_folder.substr(0, child_folder.size()-1);
    size_t found = smallstr.rfind("/");
    if(found != string::npos) {
        return parent_store[child_folder.substr(0, found+1)];
    }
    else
        return NULL;
}

void and_boolarrays(int* parent, int* current) {
    for (int i = 0; i < n_threads; i ++)
        parent[i] = parent[i] & current[i];
}

void cmp_fs() {
    map<string, MajorityFolder*> parent_store;

    int all_work_cond = 1;
    int num_threads_end = 0;
    int *cond_mask, cond_val;

    MajorityFolder *current_mf, *parent_mf;
    current_mf = NULL;
    int smf_count;
    int exit_thread, current_field, next_child, next_queue, min_index, max_equal_index;
    string current_dir_root;

    int t_num, num_child, current_child, current_fd;
    struct dirent **child_list;
    struct stat st_buff;
    string possible_majority_pathname;
    int possible_majority_path_is_file;
    int possible_majority_filesize;

    int *equal_index, *count_equal, *equal_index_copy;
    equal_index = new int[n_threads];
    equal_index_copy = new int[n_threads];
    count_equal = new int[n_threads];

    int meh = 0;

    string current_dir;

    exit_thread = 0;
    current_field = 0;
    next_child = 1;
    next_queue = 1;

    current_fd = -1;
    num_child = 0;
    current_child = 0;
    child_list = NULL;

    #pragma omp parallel default(shared) private(t_num, current_dir_root, st_buff) \
    firstprivate(current_fd, current_child, child_list, num_child) \
    num_threads(n_threads)
    {
        t_num = omp_get_thread_num();

        while(!exit_thread) {

            // condition to test if the thread has to do any work
            if (all_work_cond || cond_mask[t_num] == cond_val) {

                if (next_queue) {
                    cleanup_filelist(child_list, num_child);

                    buffer[t_num].is_finished = 0;
                    current_child = -1;
                    current_dir_root = buffer[t_num].base_path + bfs_queue.front()->rel_path;

                    num_child = scandir(current_dir_root.c_str(), &child_list, &dotfilter, alphasort);

                }

                if (next_child) {
                    if (current_fd != -1) {
                        close(current_fd);
                        current_fd = -1;
                    }

                    current_child ++;
                }

                if (current_child == num_child) {

                    #pragma omp atomic
                    num_threads_end ++;

                    buffer[t_num].is_finished = 1;

                } else {

                    switch (current_field) {

                        case 0: {
                            strcpy((char*)buffer[t_num].buff, child_list[current_child]->d_name);
                            stat((current_dir_root + child_list[current_child]->d_name).c_str(), &st_buff);
                            buffer[t_num].is_file = S_ISREG(st_buff.st_mode);
                            buffer[t_num].size = st_buff.st_size;

                            break;
                        }

                        case 1: {
                            break;
                        }

                        case 2: {

                            current_fd = open((current_dir_root + child_list[current_child]->d_name).c_str(), O_RDONLY);

                            MD5_CTX mdContext;
                            unsigned char c[MD5_DIGEST_LENGTH];
                            int bytes;

                            MD5_Init(&mdContext);
                            while((bytes = read(current_fd, buffer[t_num].buff, BUFFER_SIZE)) != 0) {
                                MD5_Update(&mdContext, buffer[t_num].buff, bytes);
                            }
                            MD5_Final(c, &mdContext);
                            memcpy(buffer[t_num].buff, c, MD5_DIGEST_LENGTH);
                            buffer[t_num].size = MD5_DIGEST_LENGTH;
                            lseek(current_fd, 0, SEEK_SET);

                            break;
                        }

                        case 3: {

                            int bytes;

                            bytes = read(current_fd, buffer[t_num].buff, BUFFER_SIZE);
                            buffer[t_num].size = bytes;

                            break;
                        }
                    }
                }
            }

            #pragma omp barrier

            #pragma omp single
            {
                all_work_cond = 0;
                meh = 0;

                if (next_child) {
                    num_threads_end = reset_compare(equal_index, count_equal);
                    if (num_threads_end > thread_majority) {

                        if (bfs_queue.empty()) {
                            exit_thread = 1;
                            all_work_cond = 0;  // not required. delete later
                        } else {

                            if (next_queue) {

                                while (current_mf != NULL && (current_mf->folder_count == 0)) {
                                    if (current_mf->folder_count == 0) {
                                        parent_mf = getParentFolder(parent_store, current_mf->rel_path);

                                        smf_count = 0;
                                        for (int i = 0; i < n_threads; i ++) {
                                            if (current_mf->mask[i] == 1)
                                                smf_count ++;
                                        }

                                        if (smf_count > thread_majority && current_mf->majority_count>0) {
                                            if (smf_count == n_threads && current_mf->is_common==1) {
                                                printf("%s is a common folder.\n", current_mf->rel_path.c_str());
                                            }
                                            else {
                                                printf("%s is a majority folder. File Systems : ", current_mf->rel_path.c_str());
                                                for (int i = 0; i < n_threads; i ++)
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
                                current_mf->majority_count += 1;
                                current_dir = current_mf->rel_path;

                                bfs_queue.pop();

                                while (current_mf != NULL && (current_mf->folder_count == 0)) {
                                    if (current_mf->folder_count == 0) {
                                        parent_mf = getParentFolder(parent_store, current_mf->rel_path);

                                        // print_if_majority_folder or common folder
                                        smf_count = 0;
                                        for (int i = 0; i < n_threads; i ++) {
                                            if (current_mf->mask[i] == 1)
                                                smf_count ++;
                                        }

                                        if (smf_count > thread_majority && current_mf->majority_count>0) {
                                            if (smf_count == n_threads && current_mf->is_common==1) {
                                                printf("%s is a common folder.\n", current_mf->rel_path.c_str());
                                            }
                                            else {
                                                printf("%s is a majority folder. File Systems : ", current_mf->rel_path.c_str());
                                                for (int i = 0; i < n_threads; i ++)
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
                            }


                            if (bfs_queue.empty()) {
                                exit_thread = 1;
                                all_work_cond = 0;  // not required. delete later
                            } else {
                                next_child = 1;
                                next_queue = 1;
                                current_field = 0;

                                all_work_cond = 0;
                                cond_mask = bfs_queue.front()->mask;
                                cond_val = 1;
                            }
                        }

                        meh = 1;

                    } else {

                        next_child = 0;

                    }
                }

                if (!meh) {
                    if (next_queue) {

                        while (current_mf != NULL && (current_mf->folder_count == 0)) {
                            if (current_mf->folder_count == 0) {
                                parent_mf = getParentFolder(parent_store, current_mf->rel_path);

                                // print_if_majority_folder or common folder
                                smf_count = 0;
                                for (int i = 0; i < n_threads; i ++) {
                                    if (current_mf->mask[i] == 1)
                                        smf_count ++;
                                }

                                if (smf_count > thread_majority && current_mf->majority_count>0) {
                                    if (smf_count == n_threads && current_mf->is_common==1) {
                                        printf("%s is a common folder.\n", current_mf->rel_path.c_str());
                                    }
                                    else {
                                        printf("%s is a majority folder. File Systems : ", current_mf->rel_path.c_str());
                                        for (int i = 0; i < n_threads; i ++)
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

                    switch(current_field) {
                        case 0: {
                            min_index = compare_pathnames(equal_index, count_equal);
                            max_equal_index = find_max_index(count_equal);

                            if (max_equal_index == min_index && count_equal[max_equal_index] > thread_majority) {

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

                                    for (int i = 0; i < n_threads; i ++) {
                                        if (equal_index[i] == min_index)
                                            new_maj_folder->mask[i] = 1;
                                        else {
                                            new_maj_folder->mask[i] = 0;
                                            new_maj_folder->is_common = 0;
                                        }
                                    }

                                    bfs_queue.push(new_maj_folder);

                                    current_field = 0;
                                    next_child = true;
                                    cond_mask = equal_index;
                                    cond_val = min_index;
                                }
                                else
                                {
                                    current_field = 1;

                                    memcpy(equal_index_copy, equal_index, sizeof(int)*n_threads);
                                    remove_all_nonmin(equal_index, count_equal, min_index);

                                    cond_mask = equal_index_copy;
                                    cond_val = min_index;
                                }

                            } else {
                                current_mf->is_common = 0;

                                next_child = 1;
                                cond_mask = equal_index;
                                cond_val = min_index;
                            }

                            break;
                        }
                        case 1: {
                            compare_sizes(equal_index, count_equal);
                            max_equal_index = find_max_index(count_equal);

                            if (count_equal[max_equal_index] > thread_majority)
                            {
                                current_field = 2;
                                possible_majority_filesize = buffer[max_equal_index].size;
                                remove_all_nonmin(equal_index, count_equal, max_equal_index);

                                cond_mask = count_equal;
                                cond_val = 1;
                            }
                            else
                            {
                                current_mf->is_common = 0;

                                next_child = true;
                                current_field = 0;
                                cond_mask = equal_index_copy;
                                cond_val = min_index;
                            }

                            break;
                        }
                        case 2: {
                            compare_hash(equal_index, count_equal);
                            max_equal_index = find_max_index(count_equal);

                            if (count_equal[max_equal_index] > thread_majority)
                            {
                                current_field = 3;
                                remove_all_nonmin(equal_index, count_equal, max_equal_index);

                                cond_mask = count_equal;
                                cond_val = 1;
                            }
                            else
                            {
                                current_mf->is_common = 0;

                                next_child = true;
                                current_field = 0;

                                cond_mask = equal_index_copy;
                                cond_val = min_index;
                            }

                            break;
                        }
                        case 3: {
                            compare_filecontent(equal_index, count_equal);
                            max_equal_index = find_max_index(count_equal);

                            if (possible_majority_filesize < BUFFER_SIZE)
                            {
                                if (count_equal[max_equal_index] > thread_majority)
                                {
                                    if (count_equal[max_equal_index] == n_threads)
                                        printf("%s is a common file.\n", (current_dir + possible_majority_pathname).c_str());
                                    else {
                                        printf("%s is a majority file. File Systems : ", (current_dir + possible_majority_pathname).c_str());
                                        for (int i = 0; i < n_threads; i ++)
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

                                next_child = 1;
                                current_field = 0;
                                possible_majority_filesize = 0;

                                cond_mask = equal_index_copy;
                                cond_val = min_index;
                            }
                            else
                            {
                                possible_majority_filesize -= BUFFER_SIZE;

                                if (count_equal[max_equal_index] > thread_majority)
                                {
                                    remove_all_nonmin(equal_index, count_equal, max_equal_index);
                                    cond_mask = count_equal;
                                    cond_val = 1;
                                }
                                else
                                {
                                    next_child = true;
                                    current_mf->is_common = 0;
                                    current_field = 0;
                                    possible_majority_filesize = 0;

                                    cond_mask = equal_index_copy;
                                    cond_val = min_index;
                                }
                            }

                            break;
                        }
                    }
                }
            }
        }

        /* cleanup code*/
        cleanup_filelist(child_list, num_child);

    }

    /*empty stack*/
    while (current_mf != NULL) {
        if (current_mf->folder_count == 0) {
            parent_mf = getParentFolder(parent_store,current_mf->rel_path);

            // print_if_majority_folder or common folder
            smf_count = 0;
            for (int i = 0; i < n_threads; i ++) {
                if (current_mf->mask[i] == 1)
                    smf_count ++;
            }

            if (smf_count > thread_majority && current_mf->majority_count>0) {
                if (smf_count == n_threads && current_mf->is_common==1) {
                    printf("%s is a common folder.\n", current_mf->rel_path.c_str());
                }
                else {
                    printf("%s is a majority folder. File Systems : ", current_mf->rel_path.c_str());
                    for (int i = 0; i < n_threads; i ++)
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

    /*free memory*/
    delete equal_index;
    delete equal_index_copy;
    delete count_equal;
}

int main(int argc, char* argv[]) {

    if (argc < 3)
    {
        printf("Atleast 2 folders are required for comparison\n");

        return 0;
    }

    n_threads = argc - 1;
    thread_majority = n_threads/2;

    buffer = new Buffer[n_threads] ();

    int i;
    for (i = 0; i < n_threads; i ++) {
        buffer[i].base_path = argv[i+1];
    }

    MajorityFolder* q = new MajorityFolder;
    q->rel_path = "/";
    q->is_common = 1;
    q->folder_count = 0;
    for (i = 0; i < n_threads; i ++)
    {
        q->mask[i] = 1;
    }
    bfs_queue.push(q);

    cmp_fs();

    delete [] buffer;

    return 0;
}
