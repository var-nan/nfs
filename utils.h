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

#include <thread>
#include <atomic>
#include <mutex>


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

#define MASTER_CLIENT_PORT 12345
#define MASTER_SERVER_PORT 22345
#define SERVER_CLIENT_PORT 32345


typedef struct {
    uint32_t size; // size of chunk.
    uint32_t offset; // offset of chunk from the start
    void *start; // starting address of chunk.
    int fd; // file descriptor.
} file_chunk;

typedef struct {
    uint32_t file_size;
    string file_name;
} file_stat;

typedef struct {
    uint32_t ip_addr;
} server;

void make_fd_nb(int fd){

}
void die(const string& s){

}

// enum RESPONSE{
//     ACCEPTED = 0x0u,
//     INSUFFICIENT_SPACE = 0x1u,
//     OKAY = 0x2u,
//     FILE_NOT_FOUND = 0x4u,
//     FILE_FOUND = 0x5u
// };

// enum REQUEST{
//     UPLOAD = 0x0u,
//     DOWNLOAD = 0x1u,
//     UPLOAD_ACK = 0x2u,
//     UPLOAD_FAILED = 0x4u,
// };



enum class MASTER_CLIENT : uint32_t{
    ACCEPTED = 0X0U,
    INSUFFICIENT_SPACE = 0X1U,
    OKAY = 0X2U,
    FILE_NOT_FOUND = 0X4U,
    FILE_FOUND = 0X5U,

    UPLOAD = 0X7U,
    DOWNLOAD = 0X8U,
    UPLOAD_ACK = 0X9U,
    UPLOAD_FAILED = 0xAU
};

enum class MASTER_SERVER : uint32_t{
    OKAY = 0X2u,
};

enum class SERVER_CLIENT : uint32_t{
    UPLOAD = 0x0u,
    DOWNLOAD = 0X1U,
    FILE_DELETE = 0X2U,
    OKAY = 0X4U,
    FILE_NOT_FOUND = 0X5U,
    ERROR = 0x6U
};

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