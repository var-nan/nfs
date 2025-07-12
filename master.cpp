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
            pfd.events |= ((cs->connection.want_read ? POLLIN : 0) | (cs->connection.want_write) ? POLLOUT : 0);
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
            if (ServerConnection *cs = handleNewConnection(fd)){
                if(chunk_servers.size() <= (size_t)(cs->connection).fd){
                    chunk_servers.resize(cs->connection.fd+1);
                }
                chunk_servers[cs->connection.fd] = cs;
                std::cout << "New chunk server added. fd:" << cs->connection.fd << std::endl;
                // send id to the chunk server.
                write(cs->connection.fd, (void *)&cur_server, sizeof(cur_server));
                cur_server++;
                
            }
        }
        // continue; // servers will not send any data to master.

        // handle the other sockets
        size_t nProcessed = 0; // early stop if all ready sockets are processed.
        for(size_t i = 1; i < poll_args.size(); i++){
            uint32_t ready = poll_args[i].revents;
            if (!ready) continue;

            ServerConnection *cs = chunk_servers[poll_args[i].fd];
            if(ready & POLLIN) { // socket is ready for reading.
                assert(cs->connection.want_read);
                handleServerRead(cs);
            }
            // no need of additional handling of write calls.
            // if (ready & POLLOUT){
            //     assert(cs->connection.want_write);
            //     handleServerWrite(cs);
            // }
            // close the socket from socket error or appliation logic. ideally chunk servers should not crash.
            if((ready & POLLERR) || cs->connection.want_close){
                (void) close(cs->connection.fd);
                std::cout << "Error from server: " << cs->connection.fd << std::endl;
                // call destructors.
            }
        }
    } // should never reach here. the blockign call.
}


ServerConnection *Master::handleNewConnection (int fd) {
    
    struct sockaddr_in client = {};
    socklen_t addrlen = sizeof(client);

    int connfd = accept(fd, (sockaddr *)&client, &addrlen);

    if (connfd < 0){
        logger.msg_errno("accept() error");
        return nullptr;
    }

    // print server information
    uint32_t ip = client.sin_addr.s_addr;
    // add log message about server ip address.

    make_fd_nb(connfd); // set to nonblocking
    // create new connection object.
    ServerConnection *cs = new ServerConnection();
    cs->connection.fd = connfd;
    cs->connection.want_read = true;
    // TODO:: initialize buffer
    return cs;
}

void Master::handleServerRead(ServerConnection *cs){
    // do a non-blocking read
    Byte buffer[sizeof(ServerInfo)]; // buffer
    ssize_t nread = read(cs->connection.fd, buffer, sizeof(buffer));

    if(nread < 0 && errno == EAGAIN) return ; // socket is not ready for reading yet.
    if (nread < 0) {
        logger.msg_errno("read() error");
        cs->connection.want_close = true;
        return ;
    }

    if (nread == 0) { // might be end of file, 
        // close connection.
        return ;
    }

    // buffer should contain ServerInfo object. parse this request.
    Byte out_buffer[] = "okay"; // response is typically okay.

    ssize_t nwrite = write(cs->connection.fd, out_buffer, sizeof(out_buffer));
    if(nwrite < 0 && errno == EAGAIN) return ; // actually not ready for writing. write buffer might be full. shouldn't happen this case.
    if (nwrite < 0) {
        logger.msg_errno("write() error");
        cs->connection.want_close = true;
        return ;
    }

    cs->connection.want_read = true;
}


void Master::startAcceptingClients(){
    int fd = socket(AF_INET, SOCK_STREAM, 0); // listening socket
    if (fd < 0)
        die("socket()");

    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1235);
    addr.sin_addr.s_addr = ntohl(0); // wildcard address.

}

int main(){
    Master master;
    master.start();
    return 0;
}
