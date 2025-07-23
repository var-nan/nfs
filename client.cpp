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
            // BUG:: last iteration is reading more data than it should,
            // incrementing the marker. The size value should be changed for last iteration.
            size_t buff_sz = (nleft > sizeof(buffer)) ? sizeof(buffer): nleft;
            if ((nread = read(chunk.fd, buffer, buff_sz)) < 0){
                logger.msg_errno("read()");
                return false;
            }
            nleft -= nread;
            nsent += nread;
            if (ssize_t nwrite = write(connectionFd, buffer, nread); nwrite != nread){
                logger.msg_errno("read()");
                return false;
            }
        }
        std::cout << "Sent file data to the server: " << nsent << " bytes." << std::endl;
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
            fc.offset = i*chunk_size; // TODO: when moved to multithreaded, move this to offset.
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

    void download(handle_t handle, std::string filename){
        // send donwload request to the file.
        int masterfd = connectToMaster();

        enum MASTER_CLIENT request = MASTER_CLIENT::DOWNLOAD;
        uint32_t buffer[2] = {static_cast<uint32_t>(request), handle};
        write(masterfd, (const void *)buffer, sizeof(buffer));

        // should get a list of servers.
        enum MASTER_CLIENT response;
        read(masterfd, &response, sizeof(response));

        if(response == MASTER_CLIENT::FILE_NOT_FOUND){
            std::cout << "File not found" << std::endl;
            close(masterfd);
            return;
        }
        
        // read server information.
        uint32_t nservers;
        if(ssize_t nread = read(masterfd, (void *)&nservers, sizeof(nservers)); nread < 0){
            logger.msg_errno("read() while reading number of servers. Error: ");
            return ;
        }

        std::vector<std::tuple<ip_addr, uint32_t, uint32_t>> servers(nservers);
        // pavani nandhan.
        size_t sz = sizeof(std::tuple<ip_addr, uint32_t, uint32_t>) * nservers;
        if(ssize_t nread = read(masterfd, (void *)servers.data(), sz); nread != sz){
            if (nread > 0) logger.incomplete_read(sz, nread);
            else logger.msg_errno("read() while reading server information. Error: ");
            return;    
        }
        
        // open file for writing.
        int fd = open(filename.data(), O_CREAT | O_RDWR, 0644);
        if (fd < 0){
            logger.msg_errno("open() while connecting to chunk server. Error: ");
            return;
        }

        uint32_t handle_id = 0;
        bool broken_download = false;

        for (const auto& [addr, chunk_size, port] : servers){
            // contact server 
            int serverfd = socket(AF_INET, SOCK_STREAM, 0);
            if(serverfd < 0){
                broken_download = true;
                break;
            }

            struct sockaddr_in sock_addr;
            sock_addr.sin_port = ntohs(port);
            sock_addr.sin_family = AF_INET;
            sock_addr.sin_addr.s_addr = ntohl(addr);

            if(connect(serverfd, (const sockaddr *)&sock_addr, sizeof(sock_addr))){
                logger.die("Couldn't connect to server");
                broken_download = true;
                break;
            }

            // send request to server
            uint32_t buff[3] = {static_cast<uint32_t>(SERVER_CLIENT::DOWNLOAD), handle, handle_id++};
            if(ssize_t nwrite = write(serverfd, buff, sizeof(buff)); nwrite != sizeof(buff)){
                broken_download = true;
                break;
            }

            // read for response.
            enum SERVER_CLIENT response;
            if(ssize_t nread = read(serverfd, (void *)&response, sizeof(response)); nread < 0){
                broken_download = true;
                break;
            }

            if (response == SERVER_CLIENT::FILE_NOT_FOUND){
                // break from this and delete the file.
                broken_download = true;
                break;
            }

            size_t ncopied = 0, nread = 0;
            Byte buffer[16 * 1024];
            while((ncopied < chunk_size) && ((nread = read(serverfd, buffer, sizeof(buffer))) != 0)){
                if(nread < 1){
                    // handle error
                    broken_download = true;
                    break;
                }
                ncopied += nread;
                if(ssize_t nwrite = write(fd, buffer, nread); nwrite != nread){
                    broken_download = true;
                    break;
                }
            }
        }

        // if download is failed, delete the file.
        if (broken_download) {
            unlink(filename.data());
            std::cout << "Download failed." << std::endl;
        }
        else {
            std::cout << "File downloaded" << std::endl;
        }

        close(masterfd);
    }
};


int main(){
    string filename = "/home/nandgate/javadocs/cppdocs/sharder/data/new_text_file";

    Client client;

    std::string msg  = "\n\nEnter a number:\n1 - upload a file\n2 - list all files\n";
        msg += "3 - delete file (enter file handle)\n";
        msg += "4 - download a file (enter file handle)\n";

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
        else if (input == 4) {
            handle_t handle;
            std::cin >> handle;
            client.download(handle, "downloaded");
        }
    }
}