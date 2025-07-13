#ifndef UTILS_H
#define UTILS_H

// stdlib
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <ctype.h>

// system headers
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/ip.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/stat.h>

#include <vector>
#include <string>
#include <unordered_map>
#include <iostream>


using namespace std;

typedef uint8_t Byte;
typedef uint32_t ip_addr;

typedef struct Buffer {
    Byte *buff_begin;
    Byte *buff_end;
    Byte *data_begin;
    Byte *data_end;

    void append(const Byte *start, size_t n);
} Buffer;


class Connection {
public:
    int fd = -1;
    bool want_read = false;
    bool want_write = false;
    bool want_close = false;

    Buffer incoming;
    Buffer outgoing;

    Connection() = default;

    void read(); // read from the fd and store to write buffer.
    void write(); // write from outgoing buffer to the socket.

    ~Connection(){
        free(incoming.buff_begin);
        free(outgoing.buff_begin);

    }
};

class ServerInfo{
public:
    uint32_t value; // IP address?
    uint32_t max_space;
    uint32_t used;
    uint32_t files;

    ServerInfo() = default;
};

class ServerConnection{
public:
    ServerConnection () = default;

    Connection connection;
    ServerInfo info;
    ip_addr address;
    // need to define destructor?
};


typedef struct {
    size_t size; // size of chunk.
    size_t offset; // offset of chunk from the start
    void *start; // starting address of chunk.
    int fd; // file descriptor.
} file_chunk;

typedef struct {
    size_t file_size;
    string file_name;
} file_stat;

typedef struct {
    uint64_t ip_addr;
} server;

void make_fd_nb(int fd){

}
void die(const string& s){

}

class Logger {
    std::string entity;
public:

    Logger () = default;
    
    Logger(std::string name) : entity{name}{}

    void msg_errno(const string& message){ std::cout << message << std::endl;}
    void die(const string& message) {
        std::cout << message << std::endl;
    }
    void message(const string& msg){ std::cout << msg << std::endl;}
    void error(const string& message) {
        std::cout << message << std::endl; 
    }
};


#endif // UTILS_H