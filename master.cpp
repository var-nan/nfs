#include "master.h"

void Master::start() {
    // start the master server.

    // register the server as non-blocking.
    int fd = socket(AF_INET, SOCK_STREAM, 0); // listening socket
    if (fd < 0)
        die("socket()");

    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)); // reuse address and port number

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0); // wild card address

    // bind the socket
    if(bind(fd, (const sockaddr *)&addr, sizeof(addr))) 
        die("bind()");

    make_fd_nb(fd); // make this socket non-blocking;

    // listen
    if(listen(fd, SOMAXCONN)) 
        die("listen()");

    printf("Master started listening...\n");

    std::vector<ServerConnection *> chunk_servers;

    // map of all chunk servers.
    std::vector<struct pollfd> poll_args;
    uint32_t cur_server = 0;

    while (true){
        // prepare the arguments for the poll
        poll_args.clear();
        
        // listening socket in the first place
        struct pollfd listeningSocket = {fd, POLLIN, 0};
        poll_args.push_back(listeningSocket);

        // put connected connted sockets in poll_args
        for (ServerConnection *cs : chunk_servers){
            if (!cs) continue;

            // always poll for error
            struct pollfd pfd = {cs->connection.fd, POLLERR, 0};
            pfd.events |= ((cs->connection.want_read ? POLLIN : 0) 
                        | ((cs->connection.want_write) ? POLLOUT : 0));
            poll_args.push_back(pfd);

        }
        // wait for any one of the event.
        int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), -1);
        /* poll can get interrupt signal EINTR from the timer after the specified timeout, before it can 
        read any connection flag. Thus it is not an acutal error.*/

        if (rv < 0 && errno != EINTR)
            die("poll()");
        if (rv == 0) continue; // none of the connections has an update.

        // check if listening socket has any new connections
        if (poll_args[0].revents){
            /* for a new connection from server, the server will send its information
                as ServerInfo object. The master assigns a id and sends it to as response.*/
            if (ServerConnection *cs = handleNewConnection(fd)){
                if(chunk_servers.size() <= (size_t)(cs->connection).fd){
                    chunk_servers.resize(cs->connection.fd+1);
                }
                chunk_servers[cs->connection.fd] = cs;
                logger.message("Server added. Fd: "+ to_string(cs->connection.fd)
                    + " Ip: "+to_string(cs->address));
                read(cs->connection.fd, (void *)&(cs->info),sizeof(cs->info));

                write(cs->connection.fd, (const void *)&cur_server, sizeof(cur_server));
                cur_server++; // TODO: change index from fd to cur_server.
                
                if (cur_server == 2){ // master can service client requests.
                    acceptClients.store(true);
                    acceptClients.notify_one();
                }
            }
        }
        // TODO: recompute available space based across all servers on the heartbeat messages.

        /* the servers periodically sends its status to master as heart-beat messages.*/
        for(size_t i = 1; i < poll_args.size(); i++){
            uint32_t ready = poll_args[i].revents;
            if (!ready) continue;

            ServerConnection *cs = chunk_servers[poll_args[i].fd];
            if(ready & POLLIN) { // socket is ready for reading.
                assert(cs->connection.want_read);
                handleServerRead(cs);
            }
           
            // close the socket when error occurs. ideally chunk servers should not crash.
            if((ready & POLLERR) || cs->connection.want_close){
                (void) close(cs->connection.fd);
                std::cout << "Error from server: " << cs->connection.fd << std::endl;
                // call destructors.
            }
        }
    } // should never reach here. poll() is a blocking call. 
}


ServerConnection *Master::handleNewConnection (int fd) {
    
    struct sockaddr_in client = {};
    socklen_t addrlen = sizeof(client);

    int connfd = accept(fd, (sockaddr *)&client, &addrlen);
    if (connfd < 0){
        logger.msg_errno("accept() error");
        return nullptr;
    }

    make_fd_nb(connfd);

    // create new connection object.
    ServerConnection *cs = new ServerConnection();
    cs->connection.fd = connfd;
    cs->connection.want_read = true;
    cs->address = client.sin_addr.s_addr;
    // TODO:: initialize buffer
    return cs;
}

void Master::handleServerRead(ServerConnection *cs){

    ServerInfo heartbeat;
    ssize_t nread = read(cs->connection.fd, (void *)&heartbeat, sizeof(heartbeat));
    if (nread < 0) {
        logger.msg_errno("read() error during heartbeat");
        cs->connection.want_close = true;
        return ;
    }
    else if (nread == 0)
        return ;
    
    logger.message("Heartbeat: " + std::to_string(cs->address) + " Files: " +
        std::to_string(heartbeat.files)+ ". Used:" + std::to_string(heartbeat.used) + 
        " bytes. Max Space: " + std::to_string(heartbeat.max_space)+" bytes.");

    // update server info locally.
    cs->info = heartbeat;

    /*  The communication between the master and the server is one-direction. The assumption 
        is that the master never crashes. So the server always finds the master.
    */
    cs->connection.want_read = true;
}


void Master::startAcceptingClients(){

    acceptClients.wait(false);
    std::cout << "Master can start accpeting clients" << std::endl;
    int fd = socket(AF_INET, SOCK_STREAM, 0); // listening socket
    if (fd < 0)
        die("socket()");

    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1235);
    addr.sin_addr.s_addr = ntohl(0); // wildcard address.

    // start accepting clients
    if(bind(fd, (const sockaddr *)&addr, sizeof(addr)) < 0)
        logger.die("bind()");
    
    if(listen(fd, SOMAXCONN) < 0)
        logger.die("listen()");

    std::cout << "Master started listening for client connections" << std::endl;

    std::vector<Byte> filename_buffer;
    std::vector<std::pair<ip_addr, uint32_t>> chunks_buffer;

    uint32_t next_handle = 0;

    // TODO: instead of multiple read/write calls, use a very large buffer.

    while(true){
        // all client connections are one-time.
        /*
            upload <filename> <filesize> -> status <filehandle> <nservers> [<server_1, chunksize>, <server_2, chunksize> ... ]
            upload_ack <filehandle> -> status
            download <filehandle> -> status <nservers> [<server_1, chunksize>, <server_2, chunksize>]
        */

        struct sockaddr_in addr;
        socklen_t size;
        int clientfd = accept(fd, (sockaddr *)&addr, &size);
        
        if (clientfd < 0)
            logger.die("accept");

        std::cout << "Connected a client" << std::endl;

        // read from client
        int request_type;
        if(ssize_t nread = read(clientfd, &request_type, sizeof(request_type)); nread < 0) {
            // break;
        }
        
        if(request_type == REQUEST::UPLOAD) { // upload
            // read filename and filesize.
            uint32_t filesize;
            read(clientfd, &filesize, sizeof(filesize));
            
            if(!is_enough_space(filesize)){
                uint32_t status = RESPONSE::INSUFFICIENT_SPACE;
                write(clientfd, &status, sizeof(status));
                goto close_conn;
            }

            uint32_t filenamesize;
            read(clientfd, &filenamesize, sizeof(filenamesize));
            
            if(filename_buffer.size() < filenamesize)  // resize filename buffer.
                filename_buffer.resize(filenamesize);
            read(clientfd, filename_buffer.data(), filenamesize);

            // return list of servers
            uint32_t status = RESPONSE::OKAY;
            uint32_t handle = next_handle++;

            { // TODO: populate chunk_buffer based on chunk availability.
                auto& f = all_files[handle];
            
                uint32_t chunk_size = (filesize + chunks_servers.size()-1)/chunks_servers.size();
                for (size_t i = 0, rem_sz = filesize; i < chunks_servers.size(); i++, rem_sz-= chunk_size){
                    auto& file_server = chunks_servers[i];
                    file_server.info.used -= chunk_size;
                    if(rem_sz >= chunk_size)
                        f.chunks.emplace_back(file_server.address, chunk_size);
                    else f.chunks.emplace_back(file_server.address, chunk_size-rem_sz);
                }
                f.filename = std::string(filename_buffer.begin(), filename_buffer.end());
                available_space -= filesize; // dont' think necessary here. but wait. not soon.
            }
            
            const auto& chunks = all_files[handle].chunks;
            uint32_t nservers = chunks.size();

            write(clientfd, &status, sizeof(status));
            write(clientfd, &handle, sizeof(handle));
            write(clientfd, &nservers, sizeof(nservers));
            write(clientfd, chunks.data(), sizeof(chunks[0])* nservers);
        }
        else if (request_type == REQUEST::UPLOAD_ACK){ // upload ack

        }
        else if (request_type == REQUEST::UPLOAD_FAILED){
            // if uploading failed, then undo bookeeping for the file.
            // inform all servers and remove file handle.
            uint32_t fhandle;
            read(clientfd, (void *)&fhandle, sizeof(fhandle));
            // so communication must be two-sided. ughhhh.

        }
        else if (request_type == REQUEST::DOWNLOAD){ // download
            uint32_t handle;
            read(clientfd, &handle, sizeof(handle));

            auto pos = all_files.find(handle);
            if(pos == all_files.end()){
                uint32_t status = RESPONSE::FILE_NOT_FOUND;
                write(clientfd, &status, sizeof(status)); 
            }
            else {
                uint32_t status = RESPONSE::FILE_FOUND;
                const auto& chunks = (pos->second).chunks;
                uint32_t nservers = chunks.size();
                write(clientfd, &status, sizeof(status));
                write(clientfd, &nservers, sizeof(nservers));
                write(clientfd, chunks.data(), sizeof(chunks[0]) * nservers);
            }
        }

        close_conn:
            close(clientfd);
        
    }
}

bool Master::is_enough_space(uint32_t filesize){
    return filesize < available_space;
}

int main(){
    Master master;
    std::thread t1([&](){master.start();});
    std::thread t2([&](){master.startAcceptingClients();});
    t1.join();
    t2.join();
    return 0;
}
