#include <sys/stat.h>
#include <iostream>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>

#include "utils.h"

using namespace std;

int download(){

    // connect to server. on port 1234
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(12345);
    addr.sin_addr.s_addr = ntohl(0);


    if(connect(sockfd, (const sockaddr *)&addr, sizeof(addr)) < 0)
        cout << "Unable to connect to server "<< endl;

    // send a file first and then connect again
    uint32_t buff[] = {3,0, 0, 0};

    write(sockfd, buff, sizeof(buff));

    // read response
    uint32_t response[3] = {0};
    read(sockfd, response, sizeof(response));

    if(response[0] == 1) {
        cout << "start reading the data" << endl;
        
        Byte buffer[1024];
        size_t chunk_size;
        memcpy(&chunk_size, &response[1], sizeof(chunk_size));

        size_t nread_so_far = 0;
        while (nread_so_far < chunk_size){
            ssize_t nread = read(sockfd, buffer, sizeof(buffer));
            if(nread < 0) break;
            nread_so_far += nread;
            std::string resp((char *)buffer, (char*)(buffer+nread));
            cout << resp << endl;
        }
        close(sockfd);
    }
    else {
        cout << "file not found" << endl;
    }

    return 0;
}


int upload(){
    // upload file
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    
    struct sockaddr_in addr;
    addr.sin_addr.s_addr = ntohl(0);
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(12345);
    
    if (connect(sockfd, (const sockaddr *)&addr, sizeof(addr)) < 0){
        cout << "unable to connect to server" << endl;
        return 0;
    }

    std::string message = "pavani :)";
    uint32_t header[5] = {3, 0, 1, 0, message.size()};
    size_t msg_size = message.size();
    memcpy((Byte *)(header+3), &msg_size, sizeof(msg_size));

    Byte buff[1024];

    memcpy(buff, header, sizeof(header));
    memcpy(buff+sizeof(header), message.data(), message.size());

    write(sockfd, buff, (sizeof(header) + message.size()));
    cout << "Written succesfully." << endl;
    return 0;
    // download();
}


int main(){
    upload();
    download();
}