#include "utils.h"

#include <atomic>
#include <thread>
#include <iostream>

class FileObject {
public:
    uint32_t f_handle;
    uint32_t chunk_id;
    uint32_t chunk_size = 0;
};


#define BUFFER_SIZE 1024

class Server {
    // list of files that it contains.
    std::unordered_map<uint32_t, FileObject> files;

    int master_fd;
    uint32_t server_id; // id of the chunk server.

    std::atomic<bool> connected = {false};

    std::string file_prefix = ""; // path name of chunks.

    Logger log;
    ServerInfo this_server_info;

public:
    // connect to master.
    Server(std::string prefix): file_prefix{prefix} {
        this_server_info.files = 0;
        this_server_info.max_space = (1<<30);
        this_server_info.used = 0;
        this_server_info.value = 0;
    }

    void start(){
        if((master_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
            return ; // couldn't connect to master.

        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = ntohs(1234);
        addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);

        if (connect(master_fd, (const sockaddr *)&addr, sizeof(addr))){
            return ; // couldn't connect to master.
        }
        // send server info to the master.
        write(master_fd, (const void *)&this_server_info, sizeof(this_server_info));

        uint32_t id; // receive id from the master.
        read(master_fd, (void *)&server_id, sizeof(server_id));

        std::cout << "Connected to master, id: " << server_id << std::endl;

        // close(master_fd);
        // return ;

        connected.store(true);

        // MAKE THIS CONNECTION NON-BLOCKING.
        std::vector<int> client_connections;
        std::vector<struct pollfd> poll_args;

        size_t hb = 0;
        while (true){
            // periodically send heartbeats to the master.
            sleep(2);
            write(master_fd, (const void *)&this_server_info, sizeof(this_server_info));
            std::cout << "sent heartbeat " << hb++ << std::endl;
            // TODO: Handle error later.
        }

    }

    void acceptClients() {
        // create nw ;
        int client_socket = socket(AF_INET, SOCK_STREAM, 0);
        if(client_socket < 0) return; // stop program.

        int val = 1;
        setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = ntohs (12345);
        addr.sin_addr.s_addr = ntohl(0);

        bind(client_socket, (const sockaddr *)&addr, sizeof(addr)); // bind the address to this socket.

        make_fd_nb(client_socket);

        if(listen(client_socket, SOMAXCONN)< 0){
            log.die("listen()");
        }
        log.message("Chunk server ready to receive connections");


        while (true){ // accept new client and service its request.
            struct sockaddr_in client;
            socklen_t addrlen = sizeof(client);
            int client_fd = accept(client_socket, (sockaddr *)&client, &addrlen);
            if(client_fd < 0){
                log.die("accept()");
            }
            
            log.message("Client connected" + to_string(client.sin_addr.s_addr));
            
            /*
                upload: file_handle(4) chunk_id(4) 1(4) chunk_size(8) <data>......
                get:    file_handle(4) chunk_id(4) 0(4) 
            */

            uint32_t buff[5] = {0}; // TODO: make sure that get request also have five uinsinged ints.

            read(client_fd, static_cast<void *>(buff), sizeof(buff));

            uint32_t f_handle = buff[0], chunk_id = buff[1], request = buff[2];

            cout << "C- file handle: " << f_handle << " chunk_id: " << chunk_id << " request: " << request << endl;

            if (request) { // upload request
                std::string file_name = file_prefix + to_string(f_handle) + "_" + to_string(chunk_id);
                int chunk_fd = open(file_name.data(), O_CREAT|O_WRONLY, 0644); // open file with permissions.
                if(chunk_fd < 0){
                    // return error response to client.
                    uint32_t response[2] = {0,0};
                    write(client_fd, response, sizeof(response));
                    close(client_fd);
                    log.die("open()");
                    continue;
                }

                size_t chunk_size;
                const size_t OFFSET = 3; // three unsigned ints.
                memcpy(static_cast<void *>(&chunk_size), static_cast<void *>(buff + OFFSET), sizeof(chunk_size));
                cout << "file handle: " << f_handle << " Chunk size " << chunk_size << endl;

                Byte buffer[BUFFER_SIZE];
                size_t ncopied = 0;
                
                while(ncopied < chunk_size){
                    ssize_t nread = read(client_fd, static_cast<void *>(buffer), sizeof(buffer));
                    if (nread < 0) {
                        cout << "read failed" << endl;
                        break;
                    }
                    write(chunk_fd, static_cast<void *>(buffer), nread);
                    ncopied += nread;
                }
                close(chunk_fd); 
                
                // create file object for this chunk.
                FileObject fobj;
                fobj.f_handle = f_handle;
                fobj.chunk_id = chunk_id;
                fobj.chunk_size = chunk_size;
                files.insert({f_handle, fobj});

                // write response to client
                uint32_t response[2] = {1, 0};
                write(client_fd, static_cast<void *>(response), sizeof(response));
                cout << "Write completed" << endl;
            }
            else { // read request.
                std::string filename = file_prefix + to_string(f_handle) + "_" + to_string(chunk_id);
                int chunk_fd = open(filename.data(), O_RDONLY);
                if(chunk_fd < 0) {
                    uint32_t response[2] = {0,0};
                    write(client_fd, static_cast<void *>(response), sizeof(response));
                    log.die("open()");
                    close(client_fd);
                    continue;
                }

                Byte buffer[BUFFER_SIZE];
                size_t nsent = 0, chunk_size = files[f_handle].chunk_size;
                cout << "Sending " << chunk_size << " bytes" << endl;

                uint32_t response[3] = {1, 0,0};
                memcpy(static_cast<void *>(&response[1]), static_cast<void *>(&chunk_size), sizeof(chunk_size));
                write(client_fd, static_cast<void *>(response), sizeof(response));

                while (nsent < chunk_size){
                    ssize_t nread = read(chunk_fd, static_cast<void *>(buffer), sizeof(buffer));
                    if (nread < 0) { break; }
                    nsent += nread;
                    write(client_fd, static_cast<void *>(buffer), nread);
                }
                close(chunk_fd);
                log.message("sent " + to_string(chunk_size) + " bytes to client");
            }
            close(client_fd);
        }
    }
    // destructor should close master connections.
};

int main(){
    Server server("data/");
    server.start();
    //server.acceptClients();
}
