#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include <vector>

using namespace std;

void comparefiles(const string& filename){
    int fd = open(filename.data(), O_RDONLY);
    if(fd < 0) {
        cout << "file cannot be opened for verification" << endl;
        return ;
    }
    int fd2 = open("duplicate_file", O_RDONLY);
    if (fd2 < 0){
        cout << "file cannot be opened for verfication" << endl;
        return ;
    }

    struct stat file_info, dup_file_info;
    fstat(fd, &file_info);
    fstat(fd2,&dup_file_info);
    size_t file_size = file_info.st_size;
    size_t dup_file_size = dup_file_info.st_size;

    if (file_size != dup_file_size) {
        cout << "different file sizes." << endl;
        return;
    }

    cout << "file size: " << dup_file_size << endl;
    
    vector<uint8_t> buffer1(file_size, 0);
    vector<uint8_t> buffer2(dup_file_size,0);

    ssize_t nread = read(fd, buffer1.data(), file_size);
    if (nread < 0){
        cout << "read() error for original file during verfification" << endl;
        return ;
    }
    ssize_t nread2 = read(fd2, buffer2.data(), dup_file_size);
    if (nread2 < 0){
        cout << "read() error for duplicate file during verfification" << endl;
        return ;
    }

    

    if(nread != nread2) {
        cout << "Read different sizes" << endl;
        return ;
    }
    cout << nread << " " << nread2 << endl;

    for (int i =0; i < nread; i++){
        if (buffer1[i] != buffer2[i]){
            cout << "bytes differ" << endl;
            return;
        }
    }
    cout << "Both files are same" << endl;
    return;
    
}

const static size_t CHUNK_SIZE = 1024 * 1; // 1kb

int main(int argc, char *argv[]){
    // given file, read individual chunks and store them seperately.
    if(argc != 2) {
        cout << "provide input file name" << endl;
        return -1;
    }
    string filename = argv[1];

    int fd = open(filename.data(), O_RDONLY);

    struct stat file_info;
    if((fstat(fd, &file_info))){
        cout << "fstat() error" << endl;
        return -1;
    }

    off_t file_size = file_info.st_size;
    size_t n_chunks = (file_size + CHUNK_SIZE-1)/CHUNK_SIZE;
    
    for (size_t i = 0; i < n_chunks; i++){
        uint8_t buffer[CHUNK_SIZE];
        ssize_t nread;
        if ((nread = read(fd, buffer, CHUNK_SIZE)) < 0){
            cout << "read() error in chunk: " << i << endl;
            return -1;
        }
        // open a new file;
        string new_file = "chunk_"+to_string(i);
        int fd2 = open(new_file.data(), O_RDWR | O_CREAT);
        if(fd2 < 0){
            cout << "open() error for chunk: " << i << endl;
            return -1;
        }
        
        struct stat chunk_info;
        fstat(fd2, &chunk_info);
        fchmod(fd2, chunk_info.st_mode | S_IRUSR | S_IWUSR);
        // read write permissio set.

        if(ssize_t nwrite = write(fd2, buffer, nread); nwrite < 0){
            cout << "write() error for chunk: " << i << endl;
            return -1;
        } 
        close(fd2);
    }

    // read individual chunks and create a duplicate file.
    int fd3 = open("duplicate_file", O_RDWR | O_CREAT);
    if (fd3 < 0) {
        cout << "error in creating the duplicate file." << endl;
        return -1;
    }

    struct stat duplicate_info;
    fstat(fd3, &duplicate_info);
    fchmod(fd3, duplicate_info.st_mode | S_IRUSR | S_IWUSR);

    for (size_t i = 0; i < n_chunks; i++){
        // open each chunks
        uint8_t buffer [CHUNK_SIZE];
        string chunk_name = "chunk_"+to_string(i);
        int fd2 = open(chunk_name.data(), O_RDONLY);
        if(fd2 < 0) {
            cout << "error in opening chunk for duplicate file" << endl;
            return -1;
        }
        ssize_t nread;
        if((nread = read(fd2, buffer, CHUNK_SIZE))< 0){
            cout << "read error () chunk " << i << endl;
            return -1;
        }
        if ((write(fd3, buffer, nread)) < 0){
            cout << "write buffer error" << endl;
            return -1;
        }
        close(fd2);
    }
    close(fd3);
    close(fd);
    cout << "File sharding completed with " << n_chunks << " chunks." << endl;

    // open two files again compare them.
    comparefiles(filename);
    
    return 0;
}