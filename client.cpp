#include "utils.h"


class Client {
    Logger logger;

    bool sendChunk(int connectionFd, const file_chunk& chunk){
        // connect to server and 
        // // mmap the page into memory
        // void *src;
        // if((src = mmap(nullptr, chunk.size, PROT_READ, MAP_SHARED, fd, chunk.offset))){
        //     logger.msg_errno("mmap error for input");
        //     return ;
        // }
        
        // use read sys call 
        Byte buffer[1024*1024*16]; // temporary buffer.
        ssize_t nread;
        size_t nleft = chunk.size;
        while (nleft> 0) {
            if ((nread = read(chunk.fd, buffer, sizeof(buffer))) < 0){
                logger.msg_errno("read()");
                return ;
            }
            write(connectionFd, chunk.start, chunk.size);
        }
        return true;
    }


    void prepare(const file_stat& file_info, int master_fd){
        uint16_t request_type;
        size_t payloadSize = sizeof(request_type) + sizeof(file_info.file_size);
        vector<Byte> buffer(payloadSize); // store extra for response.

        memcpy(buffer.data(), &request_type, sizeof(request_type));
        memcpy(&buffer[2], &file_info.file_size, sizeof(file_info.file_size));

        ssize_t nwrite = write(master_fd, buffer.data(), payloadSize);
        
        // expect from master
        buffer.clear();

        ssize_t nread = read(master_fd, buffer.data(), sizeof(request_type));
        uint16_t nservers;
        memcpy(&nservers, buffer.data(), sizeof(request_type));

        buffer.resize(nservers * sizeof(uint64_t));
        
        nread = read(master_fd, buffer.data(), nservers * sizeof(uint64_t));
         
    }

public:
    void upload(const string& filename) {
        // connect to master and send upload request
        int master_fd = socket(AF_INET, SOCK_STREAM,0);
        if(master_fd < 0)
            logger.die("socket()");

        struct sockaddr_in master_addr;
        master_addr.sin_family = AF_INET;
        master_addr.sin_port = ntohs(1233);
        master_addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);
        if (connect(master_fd, (const sockaddr *)&master_addr, sizeof(master_addr))){
            logger.die("master connect()");
            return; // couldn't connect to master, so fail operation.
        }

        // send upload request to master
            
        vector<uint32_t> servers; // result of connecting to master.
        size_t chunk_size = 0;


        for(int i = 0; i < servers.size(); i++){
            // connect to chunk server.
            int cs_fd = socket(AF_INET, SOCK_STREAM, 0); // connect to chunk server.

            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_port = ntohs(1234);
            addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);

            if(connect(cs_fd, (const struct sockaddr *) &addr, sizeof(addr))){
                logger.die("connect()");
                // server might have gone down. how to handle this?
                return;
            }

            file_chunk fc;
            fc.offset = i*chunk_size;
            sendChunk(cs_fd, fc);

            // close connection to chunk server.
            close(cs_fd);
        }

        // send commit message to master.

    }

    void listFiles();
    void donwload(int fileHandle);
};


int main(){
    string filename;

    Client client;
    client.upload(filename);
    client.listFiles();
}