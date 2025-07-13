

#include "utils.h"
#include <iostream>


using namespace std;


class Master{

    typedef struct {
        std::string filename;
        std::vector<std::pair<ip_addr, uint32_t>> chunks;
    } client_file;

    std::vector<ServerConnection> chunks_servers;
    Logger logger;

    size_t available_space;

    std::unordered_map<uint32_t, client_file> all_files;
    
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