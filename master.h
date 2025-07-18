#ifndef MASTER_H
#define MASTER_H

#include "utils.h"
#include <iostream>


using namespace std;


class Master{

    /* @brief Contains list of servers that contains the chunks of this file.
        Chunk size is also included. 
    */
    typedef struct {
        std::string filename;
        std::vector<std::tuple<ip_addr, uint32_t, uint32_t>> chunks;
        bool is_deleted = false; 
        // when the file needs to be deleted, the client should send a delete request to the master.
        // clean up thread will send the update to the servers.
    } client_file;

    std::vector<ServerConnection *> chunk_servers;
    Logger logger;

    size_t available_space;

    std::unordered_map<handle_t, client_file> all_files; // mapping of file handle and chunks information.

    std::atomic<bool> acceptClients = {false};
    std::vector<handle_t> deleted_files;
    std::mutex m;

    void deleteFile(handle_t handle);
    
public:

    Master() = default;
    
    void connect(size_t server);
    void start(); // should start and accept for chunk server connections.
    void startAcceptingClients(); // should start listening for clients.
    bool is_enough_space(uint32_t filesize);
    ServerConnection *handleNewConnection(int fd);

    void handleServerRead(ServerConnection *connection);
    void handleServerWrite(ServerConnection *connection);


};

#endif // MASTER_H