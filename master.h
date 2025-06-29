

#include "utils.h"
#include <iostream>


using namespace std;


class Master{
    std::vector<ServerConnection> chunks_servers;
    Logger logger;
public:

    Master() = default;
    
    void connect(size_t server);
    void start(); // should start and accept for chunk server connections.
    void startAcceptingClients(); // should start listening for clients.
    ServerConnection *handleNewConnection(int fd);

    void handleServerRead(ServerConnection *connection);
    void handleServerWrite(ServerConnection *connection);


};