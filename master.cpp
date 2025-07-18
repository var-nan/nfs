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
    addr.sin_port = ntohs(MASTER_SERVER_PORT);
    addr.sin_addr.s_addr = ntohl(0); // wild card address

    // bind the socket
    if(bind(fd, (const sockaddr *)&addr, sizeof(addr))) 
        die("bind()");

    make_fd_nb(fd); // make this socket non-blocking;

    // listen
    if(listen(fd, SOMAXCONN)) 
        die("listen()");

    logger.message("Master listening for servers on port "+to_string(MASTER_SERVER_PORT));

    // std::vector<ServerConnection *> chunk_servers;

    // map of all chunk servers.
    std::vector<struct pollfd> poll_args;
    uint32_t cur_server = 0;

    /* accept new server connections and receive heartbeat messages from servers. */
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
                // if(chunk_servers.size() <= (size_t)(cs->connection).fd){
                //     chunk_servers.resize(cs->connection.fd+1);
                // }
                // chunk_servers[cs->connection.fd] = cs;

                // push new connectios to the end
                chunk_servers.push_back(cs);

                logger.message("Server added. Fd: "+ to_string(cs->connection.fd)
                    + " Ip: "+to_string(cs->address) + " total: " + to_string(chunk_servers.size()));
                // read port information 
                read(cs->connection.fd, (void *)&(cs->port), sizeof(cs->port));
                read(cs->connection.fd, (void *)&(cs->info),sizeof(cs->info));

                write(cs->connection.fd, (const void *)&cur_server, sizeof(cur_server));
                cur_server++; // TODO: change index from fd to cur_server.
                
                if (cur_server == 1){ // master can service client requests.
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

            ServerConnection *cs = chunk_servers[i-1];
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
        // propagate deleted file handles to servers.
        std::lock_guard<std::mutex> lg(m);
        if(!deleted_files.empty()){
            for (const ServerConnection *conn : chunk_servers){
                auto status = MASTER_SERVER::FILE_DELETE;
                uint32_t n = deleted_files.size();
                write(conn->connection.fd, (const void *)&status, sizeof(status));
                write(conn->connection.fd, (const void *)&n, sizeof(n));
                write(conn->connection.fd, (const void *)deleted_files.data(), sizeof(deleted_files[0]) * n);
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
    cs->address = ntohl(client.sin_addr.s_addr);
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
    
    // logger.message("Heartbeat: " + std::to_string(cs->address) + " Files: " +
    //     std::to_string(heartbeat.files)+ ". Used:" + std::to_string(heartbeat.used) + 
    //     " bytes. Max Space: " + std::to_string(heartbeat.max_space)+" bytes.");

    // update server info locally.
    cs->info = heartbeat;

    /*  The communication between the master and the server is one-direction. The assumption 
        is that the master never crashes. So the server always finds the master.
    */

    // TODO: respond to heartbeat messages.
    cs->connection.want_read = true;
}


void Master::startAcceptingClients(){

    acceptClients.wait(false); // wait until servers are connected.

    int fd = socket(AF_INET, SOCK_STREAM, 0); // listening socket
    if (fd < 0)
        die("socket()");

    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(MASTER_CLIENT_PORT);
    addr.sin_addr.s_addr = ntohl(0); // wildcard address.

    // start accepting clients
    if(bind(fd, (const sockaddr *)&addr, sizeof(addr)) < 0)
        logger.die("bind()");
    
    if(listen(fd, SOMAXCONN) < 0)
        logger.die("listen()");

    logger.message("Master listening for client connections.. on " + std::to_string(MASTER_CLIENT_PORT));

    std::vector<Byte> filename_buffer;
    std::vector<std::pair<ip_addr, uint32_t>> chunks_buffer;

    handle_t next_handle = 0;

    // TODO: instead of multiple read/write calls, use a very large buffer.

    while(true){
        // all client connections are one-time.
        /*
            upload <filesize> <filenamesize> <filename> -> status <filehandle> <nservers> [<server_1, chunksize>, <server_2, chunksize> ... ]
            upload_ack <filehandle> -> status
            download <filehandle> -> status <nservers> [<server_1, chunksize>, <server_2, chunksize>]
        */

        struct sockaddr_in addr;
        socklen_t size;
        int clientfd = accept(fd, (sockaddr *)&addr, &size);
        
        if (clientfd < 0)
            logger.die("accept");

        logger.message("Client connected. " + std::to_string(addr.sin_addr.s_addr));

        // read from client
        enum MASTER_CLIENT request_type;
        if(ssize_t nread = read(clientfd, (void *)&request_type, sizeof(request_type)); nread < 0) {
            // break;
        }
        
        if(request_type == MASTER_CLIENT::UPLOAD) { // upload
            // read filename and filesize.
            uint32_t filesize;
            read(clientfd, &filesize, sizeof(filesize));
            
            if(!is_enough_space(filesize)){
                enum MASTER_CLIENT status = MASTER_CLIENT::INSUFFICIENT_SPACE;
                write(clientfd, &status, sizeof(status));
                goto close_conn; // what happens to unread data in read buffer?
            }

            uint32_t filenamesize;
            read(clientfd, &filenamesize, sizeof(filenamesize));
            
            if(filename_buffer.size() < filenamesize)  // resize filename buffer.
                filename_buffer.resize(filenamesize);
            read(clientfd, filename_buffer.data(), filenamesize);

            std::cout<< "upload - filesize: " << filesize << " file name size: " << filenamesize << std::endl;
            
            // return list of servers
            auto status = MASTER_CLIENT::OKAY;
            uint32_t handle = next_handle++;

            { // TODO: populate chunk_buffer based on chunk availability.
                auto& f = all_files[handle];

                uint32_t chunk_size = (filesize + chunk_servers.size()-1)/chunk_servers.size();
                // todo: change the order of chunk servers for each file.
                // if the last server doesn't have any chunk, the loop will exit without adding to metadata.
                for (ssize_t i = 0, rem_sz = filesize; (i < chunk_servers.size()) && (rem_sz > 0); 
                                                i++, rem_sz-= chunk_size){
                    auto file_server = chunk_servers[i];
                    file_server->info.used -= chunk_size;
                    uint32_t sz = (rem_sz >= chunk_size) ? chunk_size : rem_sz;
                    f.chunks.emplace_back(file_server->address, sz, file_server->port);
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
        else if (request_type == MASTER_CLIENT::UPLOAD_ACK){ // upload ack
            // no need to do anything.
        }
        else if (request_type == MASTER_CLIENT::DOWNLOAD){ // download
            uint32_t handle;
            read(clientfd, &handle, sizeof(handle));

            auto pos = all_files.find(handle);
            if((pos == all_files.end()) || (pos->second.is_deleted)){
                auto status = MASTER_CLIENT::FILE_NOT_FOUND;
                write(clientfd, &status, sizeof(status)); 
            }
            else {
                auto status = MASTER_CLIENT::FILE_FOUND;
                const auto& chunks = (pos->second).chunks;
                uint32_t nservers = chunks.size();
                write(clientfd, &status, sizeof(status));
                write(clientfd, &nservers, sizeof(nservers));
                write(clientfd, chunks.data(), sizeof(chunks[0]) * nservers);
            }
        }
        else if ((request_type == MASTER_CLIENT::FILE_DELETE) || 
                        (request_type == MASTER_CLIENT::UPLOAD_FAILED)){
            
            handle_t handle;
            read(clientfd, (void *)&handle, sizeof(handle));

            std::cout << "delete - file handle: " << handle << std::endl;

            auto pos = all_files.find(handle);
            auto status = MASTER_CLIENT::FILE_NOT_FOUND;
            if (pos != all_files.end()){
                status = MASTER_CLIENT::OKAY;
                pos->second.is_deleted = true; // delete the file later.
            }
            write(clientfd, (const void *)&status, sizeof(status));
        }
        else if (request_type == MASTER_CLIENT::LIST_ALL_FILES){
            // send list of all files along with their handles
            uint32_t nfiles = all_files.size();
            uint32_t file_names_size = 0;
            for (const auto& [k,v] : all_files)  file_names_size += v.filename.size();
            // buffer is nfiles (handle, filenamesize, filename) , (handle2, filenamesize, filename2) ... 
            size_t buffer_size = sizeof(nfiles) + nfiles *(sizeof(handle_t) + sizeof(uint32_t)) + file_names_size;
            std::vector<Byte> buffer(buffer_size);
            memcpy((void *)buffer.data(), &nfiles, sizeof(nfiles));
            uint32_t offset = sizeof(nfiles);
            for (const auto& [k,v] : all_files){
                uint32_t filenamesize = v.filename.size();
                memcpy(buffer.data()+ offset , (void *)&k, sizeof(k));
                offset += sizeof(k);
                memcpy(buffer.data()+ offset, (void *)&filenamesize, sizeof(filenamesize));
                offset += sizeof(filenamesize);
                memcpy(buffer.data() + offset, (void *)v.filename.data(), v.filename.size());
                offset += filenamesize;
            }
            write(clientfd, (const void *)buffer.data(), buffer.size());
        }

        close_conn:
            close(clientfd);

        // delete the files that are marked as delete.
        for (auto& p : all_files){
            if (p.second.is_deleted){
                deleteFile(p.first);
            }
        }
        
    }
}

void Master::deleteFile(handle_t handle){
    // pass the handle to other thread, it should 
    m.lock();
    deleted_files.push_back(handle);
    m.unlock();
    all_files.erase(handle);
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
