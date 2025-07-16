#include "server.h"


void Server::start(){

    master_connect.wait(false);

    if((master_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        return ; // couldn't connect to master.

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(MASTER_SERVER_PORT);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);

    if (connect(master_fd, (const sockaddr *)&addr, sizeof(addr))){
        return ; // couldn't connect to master.
    }
    // send server info to the master.
    write(master_fd, (const void *)&client_port, sizeof(client_port));
    write(master_fd, (const void *)&this_server_info, sizeof(this_server_info));

    uint32_t id; // receive id from the master.
    read(master_fd, (void *)&server_id, sizeof(server_id));

    std::cout << "Connected to master, id: " << server_id << std::endl;

    client_connect.store(true);
    client_connect.notify_one();

    // MAKE THIS CONNECTION NON-BLOCKING.
    std::vector<int> client_connections;
    std::vector<struct pollfd> poll_args;

    size_t hb = 0;
    while (true){
        // periodically send heartbeats to the master.
        sleep(2);
        write(master_fd, (const void *)&this_server_info, sizeof(this_server_info));
        // std::cout << "sent heartbeat " << hb++ << std::endl;
        // TODO: Handle error later.
    }
}

void Server::acceptClients() {

    int client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if(client_socket < 0) return; // stop program.

    int val = 1;
    setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs (SERVER_CLIENT_PORT);
    addr.sin_addr.s_addr = ntohl(0);

    bind(client_socket, (const sockaddr *)&addr, sizeof(addr)); // bind the address to this socket.

    make_fd_nb(client_socket);
   
    if(listen(client_socket, SOMAXCONN)< 0)
        log.die("listen()");

    socklen_t len = sizeof(addr);
    getsockname(client_socket, (struct sockaddr *)&addr, &len);
    client_port = ntohs(addr.sin_port);
    //now other thread can connect to master
    master_connect.store(true);
    master_connect.notify_one();

    log.message("Server ready to receive clients on " + to_string(client_port));

    client_connect.wait(false);

    while (true){ // accept new client and service its request.
        struct sockaddr_in client;
        socklen_t addrlen = sizeof(client);
        int client_fd = accept(client_socket, (sockaddr *)&client, &addrlen);
        if(client_fd < 0){
            log.die("accept()");
        }
        
        log.message("Client connected" + to_string(client.sin_addr.s_addr));
        
        /*
            upload: file_handle(4) chunk_id(4) chunk_size(8) <data>......
            get:    file_handle(4) chunk_id(4) 
            delete: file_handle(4) chunk_id(4) 
        */

        enum SERVER_CLIENT request;
        read(client_fd, static_cast<void *>(&request), sizeof(request));

        if (request == SERVER_CLIENT::UPLOAD) { // upload request

            uint32_t buff[3] = {0};
            read (client_fd, buff, sizeof(buff));

            uint32_t f_handle = buff[0], chunk_id = buff[1], chunk_size = buff[2];
            log.message("upload, file-handle: " + std::to_string(f_handle) + ", chunk id: " + 
                std::to_string(chunk_id) + " chunk size: " + std::to_string(chunk_size));

            std::string file_name = file_prefix + to_string(server_id) + "_" + to_string(f_handle) + "_" + to_string(chunk_id);
            std::cout << file_name << std::endl;

            int chunk_fd = open(file_name.data(), O_CREAT|O_WRONLY, 0644); // open file with permissions.
            if(chunk_fd < 0){
                std::cout << "Error no " << errno << std::endl;
                // return error response to client.
                auto response = SERVER_CLIENT::ERROR;
                write(client_fd, (void *)&response, sizeof(response));
                close(client_fd);
                log.die("open()");
                continue;
            }
            else {
                auto response = SERVER_CLIENT::OKAY;
                write(client_fd, (void *)&response, sizeof(response));
            }

            size_t ncopied = 0, nread = 0, nleft = chunk_size;
            Byte *buffer = (Byte *)malloc(chunk_size);
            std::cout << "Server ready to receive file chunk " << std::endl;

            // /* read to buffer from socket and write it to to file. */
            // while((ncopied < chunk_size) && ((nread = read(client_fd, (void *)buffer, chunk_size) > 0))) {
            //     write(chunk_fd, (const void *)buffer, nread);
            //     ncopied += nread;
            // }
            nread = read(client_fd, (void *)buffer, chunk_size);
            write(chunk_fd, (void *)buffer, nread);
            
            free(buffer);
            std::cout << "Server received chunk " << nread << " bytes." << std::endl;
            
            // close(chunk_fd); 
            
            // create file object for this chunk.
            FileObject fobj;
            fobj.f_handle = f_handle;
            fobj.chunk_id = chunk_id;
            fobj.chunk_size = chunk_size;
            files.insert({f_handle, fobj});

            // write response to client
            auto response = SERVER_CLIENT::OKAY;
            write(client_fd, static_cast<void *>(&response), sizeof(response));
            cout << "Write completed" << endl;
        }
        else if (request == SERVER_CLIENT::DOWNLOAD) { // read request.

            uint32_t buff[2] = {0};
            read(client_fd, buff, sizeof(buff));
            uint32_t f_handle = buff[0], chunk_id = buff[1];

            std::string filename = file_prefix + to_string(f_handle) + "_" + to_string(chunk_id);
            
            int chunk_fd = open(filename.data(), O_RDONLY);
            if(chunk_fd < 0) {
                auto response = SERVER_CLIENT::ERROR;
                write(client_fd, static_cast<void *>(&response), sizeof(response));
                log.die("open()");
                close(client_fd);
                continue;
            }

            uint32_t nsent = 0, chunk_size = files[f_handle].chunk_size;
            cout << "Sending " << chunk_size << " bytes" << endl;

            uint32_t response[3] = {1, chunk_id, chunk_size};
            write(client_fd, static_cast<const void *>(response), sizeof(response));

            Byte buffer[BUFFER_SIZE];

            /* read from disk to buffer and write it to socket. */
            while (nsent < chunk_size){
                ssize_t nread = read(chunk_fd, static_cast<void *>(buffer), sizeof(buffer));
                if (nread < 0) { break; }
                nsent += nread;
                write(client_fd, static_cast<void *>(buffer), nread);
            }
            close(chunk_fd);
            log.message("sent " + to_string(chunk_size) + " bytes to client");
        }
        else if (request == SERVER_CLIENT::FILE_DELETE){
            // delete file if exists.
        }
        close(client_fd);
    }
}

int main(){
    Server server("/home/nandgate/javadocs/cppdocs/sharder/data/");
    std::thread t{[&](){server.start();}};
    std::thread t2{[&](){server.acceptClients();}};
    t.join();
    t2.join();
    //server.acceptClients();
}
