#ifndef SERVER_H
#define SERVER_H

#include "utils.h"

class FileObject {
public:
    uint32_t f_handle;
    uint32_t chunk_id;
    uint32_t chunk_size = 0;
};

#define BUFFER_SIZE 1024

class Server{

    std::unordered_map<uint32_t, FileObject> files;
    int master_fd;
    uint32_t server_id;
    std::atomic<bool> client_connect = {false};
    std::atomic<bool> master_connect = {false};
    std::string file_prefix ; // TODO: set it
    Logger log;
    ServerInfo this_server_info;
    std::mutex mu;
    uint32_t client_port;

public:
    Server (std::string prefix): file_prefix{prefix}{
        this_server_info.files = 0;
        this_server_info.max_space = (1<<30);
        this_server_info.used = 0;
        this_server_info.value = 0;
        client_port = 0;
    }

    void start();
    void acceptClients();

    // destructors should close any open file descriptors.
};

#endif // SERVER_H