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
                return false;
            }
            nleft -= nread;
            write(connectionFd, buffer, nread);
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

        int file_fd = open(filename.data(), O_RDONLY);

        // file information.
        struct stat f_stat; 
        fstat(file_fd, &f_stat);

        file_stat fs;
        fs.file_size = f_stat.st_size;
        fs.file_name = filename;
        
        Byte *m_buffer = (Byte *) malloc(sizeof(fs.file_size) + fs.file_name.size());
        memcpy(m_buffer, &fs.file_size, sizeof(fs.file_size));
        memcpy(m_buffer +sizeof(fs.file_size), fs.file_name.data(), fs.file_name.size());

        // send upload request to master.
        if(ssize_t nwritten = write(master_fd, m_buffer, fs.file_size+fs.file_name.size()); nwritten < 0){
            logger.die("write()");
            return ;
        }

        /* get list of ip addresses of chunk servers from the master.
         * response: file_handle(4) n_servers(4) server_1(4) server_2(4) ..... server_n(4)
        */

        uint64_t payload;
        read(master_fd, &payload, sizeof(uint64_t));

        uint32_t f_handle = payload >> 32;
        uint32_t nservers = payload >> 32; // TODO: change this.

        m_buffer = (Byte *) realloc(m_buffer, nservers * sizeof(ip_addr));
        read(master_fd, m_buffer, nservers * sizeof(ip_addr));
         
        size_t chunk_size = 0;

        bool transfer_completed = true;

        for(uint32_t i = 0, offset = 0; (i < nservers) && transfer_completed; i++, offset += sizeof(ip_addr)){
            
            int cs_fd = socket(AF_INET, SOCK_STREAM, 0); // connect to chunk server.

            ip_addr server_addr;
            memcpy(&server_addr, m_buffer + offset, sizeof(ip_addr));

            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_port = ntohs(1234);
            addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);

            if(connect(cs_fd, (const struct sockaddr *) &addr, sizeof(addr))){
                logger.die("connect()"); // server might have gone down. how to handle this?
                transfer_completed = false;
                break; // close open file descriptors, master should clean up the incomplete files.
            }

            // prepare chunk server to accept data. (file_handle, chunk_id, chunk_size)
            Byte buff[sizeof(f_handle) + sizeof(i) + sizeof(chunk_size)];
            memcpy(buff, &f_handle, sizeof(f_handle));
            memcpy(buff+sizeof(f_handle), &i, sizeof(i));
            memcpy(buff + sizeof(f_handle) + sizeof(i), &chunk_size, sizeof(chunk_size));

            write(cs_fd, buff, sizeof(buff)); // send metadata to sever, and prepare it.

            file_chunk fc;
            fc.fd = file_fd;
            fc.offset = i*chunk_size;

            if (!sendChunk(cs_fd, fc)) transfer_completed = false;

            close(cs_fd);
        }

        // send commit to master.
        uint64_t status = (transfer_completed);
        status = (status << 32) + f_handle;
        write(master_fd, &status, sizeof(status));

        close(master_fd);
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