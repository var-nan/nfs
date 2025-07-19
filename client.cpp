#include "utils.h"


class Client {
    Logger logger;

    bool sendChunk(int connectionFd, const file_chunk& chunk){
        
        enum SERVER_CLIENT response;
        read(connectionFd, (void *)&response, sizeof(response));
        if(response == SERVER_CLIENT::ERROR) return false; // server shoudl say OKAY.

        // use read sys call 
        Byte buffer[1024*16]; // temporary buffer. 16 kB
        ssize_t nread = 0, nleft = chunk.size, nsent = 0;
        while (nleft> 0) {
            if ((nread = read(chunk.fd, buffer, chunk.size)) < 0){
                logger.msg_errno("read()");
                return false;
            }
            if(nread == 0) break;
            nleft -= nread;
            nsent += nread;
            write(connectionFd, buffer, nread);
        }
        // std::cout << "Sent file data to the server: " << nsent << " bytes." << std::endl;
        // if the write doesn't complete, server will send error.
        read(connectionFd, (void *)&response, sizeof(response));
        return response == SERVER_CLIENT::OKAY;
    }


    int connectToMaster() {
        int master_fd = socket(AF_INET, SOCK_STREAM,0);
        if(master_fd < 0){
            logger.die("socket()");
            return -1;
        }

        struct sockaddr_in master_addr;
        master_addr.sin_family = AF_INET;
        master_addr.sin_port = ntohs(MASTER_CLIENT_PORT);
        master_addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);
        if (connect(master_fd, (const sockaddr *)&master_addr, sizeof(master_addr))){
            logger.die("master connect()");
            return -1; // couldn't connect to master, so fail operation.
        }
        return master_fd;
    }

public:
    void upload(const string& filename) {
        // connect to master and send upload request
        int master_fd = socket(AF_INET, SOCK_STREAM,0);
        if(master_fd < 0)
            logger.die("socket()");

        struct sockaddr_in master_addr;
        master_addr.sin_family = AF_INET;
        master_addr.sin_port = ntohs(MASTER_CLIENT_PORT);
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

        uint32_t file_name_size = fs.file_name.size();
        enum MASTER_CLIENT request = MASTER_CLIENT::UPLOAD;

        size_t buff_size = sizeof(request) + sizeof(fs.file_size) + sizeof(file_name_size) + fs.file_name.size();
        Byte *m_buffer = (Byte *) malloc(buff_size);
        memcpy((void *)(m_buffer), &request, sizeof(request));
        memcpy((void *)(m_buffer + sizeof(request)), (void *)(&fs.file_size), sizeof(fs.file_size));
        memcpy((void *)(m_buffer + sizeof(request) + sizeof(fs.file_size)), (void *)(&file_name_size), sizeof(file_name_size));
        memcpy((void *)(m_buffer + sizeof(request) + sizeof(fs.file_size) + sizeof(file_name_size)), 
                    (void *)(fs.file_name.data()), fs.file_name.size());
        
        // send upload request to master.
        if(ssize_t nwritten = write(master_fd, (void *)(m_buffer), buff_size); nwritten < 0){
            logger.die("write()");
            return ;
        }
        // std::cout << "Sent upload request - filesize: " << fs.file_size << " name: " << fs.file_name.size() << std::endl;
        // return ;

       /* response: <status> <file_handle> <nservers> <server_1,size_1> .... <server_n, size_n>
          if above response is valid if the status is OKAY.
       */

        enum MASTER_CLIENT response_status;
        uint32_t f_handle, nservers; // status, file_handle, nservers
        read(master_fd, (void *)&response_status, sizeof(response_status));
        if(response_status == MASTER_CLIENT::INSUFFICIENT_SPACE) return;

        read(master_fd, (void *)&f_handle, sizeof(f_handle));
        read(master_fd, (void *)&nservers, sizeof(nservers));

        uint32_t payload_size = nservers * (sizeof(ip_addr) + sizeof(uint32_t) + sizeof(uint32_t));
        std::vector<std::tuple<ip_addr, uint32_t, uint32_t>> servers(nservers);
        // servers.resize(nservers);
        read(master_fd, (void *)servers.data(), payload_size);

        std::cout <<"Nservers: " << nservers << std::endl;
        for (const auto& p : servers) {
            auto [addr, chunk_size, port] = p;
            std::cout << " [" << addr << ", " << chunk_size << ", " << port << "] ";
        } std::cout << std::endl;

        bool transfer_completed = true;

        for(uint32_t i = 0, offset = 0; (i < nservers) && transfer_completed; i++, offset += sizeof(ip_addr)){
            
            auto [server_addr, chunk_size, port] = servers[i];
            
            int cs_fd = socket(AF_INET, SOCK_STREAM, 0);

            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_port = ntohs(port);
            addr.sin_addr.s_addr = ntohl(server_addr);

            if(connect(cs_fd, (const struct sockaddr *) &addr, sizeof(addr))){
                logger.die("connect()"); // server might have gone down. how to handle this?
                transfer_completed = false;
                close(cs_fd);
                break; // close open file descriptors, master should clean up the incomplete files.
            }
            std::cout << "Connected to server: " << std::endl;

            // prepare chunk server to accept data. (request_type, file_handle, chunk_id, chunk_size)
            uint32_t buff[] = {static_cast<uint32_t>(SERVER_CLIENT::UPLOAD), f_handle, i, chunk_size};
            write(cs_fd, (void *)buff, sizeof(buff));

            file_chunk fc;
            fc.fd = file_fd;
            fc.offset = i*chunk_size;
            fc.size = chunk_size;

            if (!sendChunk(cs_fd, fc)) transfer_completed = false;

            close(cs_fd);
        }

        if(transfer_completed){ // send commit to master.
            std::cout << "Write completed" << std::endl;
            uint64_t status = (transfer_completed);
            status = (status << 32) + f_handle;
            write(master_fd, &status, sizeof(status));
        }
        else { // undo partial writes to the servers.
            std::cout << "Write failed" << std::endl;
            
        }

        close(master_fd);
    }

    void listFiles(){
        int master_fd = socket(AF_INET, SOCK_STREAM, 0);
        if(master_fd < 0)
            logger.die("socket()");

        struct sockaddr_in master_addr;
        master_addr.sin_family = AF_INET;
        master_addr.sin_port = ntohs(MASTER_CLIENT_PORT);
        master_addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);
        if (connect(master_fd, (const sockaddr *)&master_addr, sizeof(master_addr))){
            logger.die("master connect()");
            return; // couldn't connect to master, so fail operation.
        }
        
        enum MASTER_CLIENT request = MASTER_CLIENT::LIST_ALL_FILES;
        write(master_fd, &request, sizeof(request));
        
        // // wait for response
        // enum MASTER_CLIENT response;
        // read(master_fd, &response, sizeof(response));
        
        uint32_t nfiles;
        read(master_fd, &nfiles, sizeof(nfiles));

        for(uint32_t i = 0; i < nfiles; i++){
            uint32_t handle;
            read(master_fd, &handle, sizeof(handle));
            uint32_t filenamesize;
            read(master_fd, &filenamesize, sizeof(filenamesize));
            std::string filename;
            filename.resize(filenamesize);
            read(master_fd, filename.data(), filenamesize);

            std::cout << handle << " " << filename << std::endl;
        }
    }

    void delete_file(handle_t handle) {
        int master_fd = connectToMaster();

        uint32_t buffer[2] = {static_cast<uint32_t>(MASTER_CLIENT::FILE_DELETE), handle};
        write(master_fd, (const void *)buffer, sizeof(buffer));

        enum MASTER_CLIENT response;
        read(master_fd, (void *)&response, sizeof(response));
        if (response == MASTER_CLIENT::OKAY){
            std::cout << "file deleted" << std::endl;
        }
        else if (response == MASTER_CLIENT::FILE_NOT_FOUND){
            std::cout << "file not found" << std::endl;
        }
        close(master_fd);
    }

    void download(handle_t handle){
        // send donwload request to the file.
        int masterfd = connectToMaster();

        enum MASTER_CLIENT request = MASTER_CLIENT::DOWNLOAD;
        uint32_t buffer[2] = {static_cast<uint32_t>(request), handle};
        write(masterfd, (const void *)buffer, sizeof(buffer));

        // should get a list of servers.
        enum MASTER_CLIENT response;
        read(masterfd, &response, sizeof(response));
        
        if(response == MASTER_CLIENT::FILE_FOUND){
            // read server information.
        }
        close(masterfd);
    }
};


int main(){
    string filename = "/home/nandgate/javadocs/cppdocs/sharder/data/chunk_3";

    Client client;
    // client.upload(filename);
    // client.listFiles();

    std::string msg  = "\n\nEnter a number:\n1 - upload a file\n2 - list all files\n";
        msg += "3 - delete file (enter file handle)\n";

    int input;
    while (true) {
        std::cout << msg << endl;
        std::cin >> input;
        if(input == 0) break;
        if (input == 1) client.upload(filename);
        else if (input == 2) client.listFiles();
        else if (input == 3) {
            handle_t handle;
            std::cin >> handle;
            client.delete_file(handle);
        }
    }
    // client.listFiles();
}