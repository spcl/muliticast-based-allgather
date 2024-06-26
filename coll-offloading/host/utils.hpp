#ifndef _UTILS_HPP_
#define _UTILS_HPP_

#include <iostream>
#include <chrono>
#include <unistd.h>
#include <limits.h>
#include <cstdio>
#include <cassert>
#include <cstring>

#include <mpi.h>

#include <sys/socket.h>
#include <arpa/inet.h>

#define CHKERR(subsyst, x)                                              \
    {                                                                   \
        int retval = (x);                                               \
        if (retval != 0) {                                              \
            fprintf(stderr, "%s fatal error: %s returned %d at %s:%d\n", \
                    #subsyst, #x, retval, __FILE__, __LINE__);          \
            exit(EXIT_FAILURE);                                         \
        }                                                               \
    }

#define CHKERR_PTR(subsyst, x, ptr)                                     \
    {                                                                   \
        ptr = (x);                                                      \
        if (!ptr) {                                                     \
        fprintf(stderr, "%s fatal error: %s failed at %s:%d\n",         \
                #subsyst, #x, __FILE__, __LINE__);                      \
            exit(EXIT_FAILURE);                                         \
        }                                                               \
    }


#ifdef BMARK_CM_MPI

#define MPI_CHKERR(mpi_fn, ...)                             \
    {                                                       \
        if (MPI_##mpi_fn (__VA_ARGS__) != MPI_SUCCESS) {    \
            fprintf(stderr, "MPI call error");              \
            exit(EXIT_FAILURE);                             \
        }                                                   \
    }

struct oob_mpi_ctx {
    int _world_size;
    int _my_rank;

    oob_mpi_ctx(int argc, char **argv)
    {
        MPI_CHKERR(Init, &argc, &argv);
        MPI_CHKERR(Comm_size, MPI_COMM_WORLD, &_world_size);
        MPI_CHKERR(Comm_rank, MPI_COMM_WORLD, &_my_rank);
    }

    ~oob_mpi_ctx()
    {
        MPI_CHKERR(Finalize, /* no args needed */);
    }

    int get_id()
    {
        return _my_rank;
    }

    void exchange(const void *sbuf, void *rbuf, std::size_t size)
    {
        auto remote_id = (_my_rank + 1) % 2; // TODO: pass id from the callee
        MPI_CHKERR(Sendrecv,
                   sbuf, size, MPI_CHAR, remote_id, 0,
                   rbuf, size, MPI_CHAR, remote_id, 0,
                   MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    // NOTE: in case the main thread didnt call constructor were dead
    void barrier()
    {
        MPI_CHKERR(Barrier, MPI_COMM_WORLD);
    }

    double time()
    {
        return MPI_Wtime();
    }
};

#endif

#define sock_msg_fn(func, modifier)                                     \
    static int sock_##func(int fd, modifier void *buf, std::size_t len) \
    {                                                                   \
        modifier char *ptr = reinterpret_cast<modifier char *>(buf);    \
        std::size_t processed = 0;                                      \
        std::size_t to_process = len;                                   \
        int ret;                                                        \
        while (processed < len) {                                       \
          ret = func (fd, ptr, to_process, 0);                          \
            if (ret < 0)                                                \
                return ret;                                             \
            ptr += ret;                                                 \
            processed += ret;                                           \
            to_process -= ret;                                          \
        }                                                               \
        return 0;                                                       \
    }                                                                   \

sock_msg_fn(send, const);
sock_msg_fn(recv, );

struct oob_sock_ctx {

    static std::string get_hostname()
    {
        char hostname[HOST_NAME_MAX];
        gethostname(hostname, HOST_NAME_MAX);
        return std::string(hostname);
    }

    void server_accept()
    {
        struct sockaddr_in client_addr;
        unsigned int client_size = sizeof(client_addr);
        _socket_data = accept(_socket_listen, (struct sockaddr *)&client_addr, &client_size);

        if (_socket_data < 0) {
            std::cerr << "Server can't accept: " << strerror(errno) << std::endl;
            exit(EXIT_FAILURE);
        }
        std::cerr << "Client connected at IP: " << inet_ntoa(client_addr.sin_addr) << std::endl;
    }

    void data_sock_close()
    {
        close(_socket_data);
    }

    void server_setup()
    {
        /* Create socket */
        _socket_listen = socket(AF_INET, SOCK_STREAM, 0);

        if (_socket_listen < 0) {
            std::cerr << "Server error while creating socket" << std::endl;
            exit(EXIT_FAILURE);
        }
        std::cerr << "Server socket created successfully" << std::endl;

        int enable = 1;
        if (setsockopt(_socket_listen, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(enable))) {
            std::cerr << "Server error while setting socket options" << std::endl;;
            exit(EXIT_FAILURE);
        }

        /* Set port and IP: */
        struct sockaddr_in server_addr;
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(_port);
        server_addr.sin_addr.s_addr = INADDR_ANY; /* listen on any interface */

        /* Bind to the set port and IP: */
        if (bind(_socket_listen, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            std::cerr << "Server couldn't bind to the port " << _port << std::endl;
            exit(EXIT_FAILURE);
        }
        std::cerr << "Server done with binding" << std::endl;

        /* Listen for clients: */
        if (listen(_socket_listen, 1) < 0) {
            std::cerr << "Error while listening" << std::endl;
            exit(EXIT_FAILURE);
        }
        std::cerr << "Listening for incoming connections on port " << _port << std::endl;

        if (!_reuse_server) {
            server_accept();
        }
    }

    void client_setup(const char *server_host)
    {
        struct sockaddr_in server_addr;

        /* Create socket */
        _socket_data = socket(AF_INET, SOCK_STREAM, 0);

        if (_socket_data < 0) {
            std::cerr << "Unable to create socket" << std::endl;
            exit(EXIT_FAILURE);
        }
        std::cerr << "Client socket created successfully" << std::endl;

        /* Set port and IP the same as server-side: */
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(_port);
        server_addr.sin_addr.s_addr = inet_addr(server_host);

        /* Send connection request to server: */
        if (connect(_socket_data, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            std::cerr << "Unable to connect to server at " << server_host << ":" << _port << std::endl;
            exit(EXIT_FAILURE);
        }
        std::cerr << "Connected with server successfully" << std::endl;
    }

    oob_sock_ctx(std::string server_addr, bool is_server, bool reuse_server, int default_port)
        :
        _is_server(is_server),
        _reuse_server(reuse_server),
        _port(default_port)
    {
        if (_is_server) {
            server_setup();
        } else {
            if (!server_addr.compare("")) {
                std::cerr << "Server address not specified" << std::endl;
                exit(EXIT_FAILURE);
            }
            client_setup(server_addr.c_str());
        }
    }

    ~oob_sock_ctx()
    {
        if (!_reuse_server || !_is_server) {
            data_sock_close();
        }
        if (_is_server) {
            close(_socket_listen);
        }
    }

    int get_id()
    {
        //FIXME: Dirty hack to make DPU server accessible from remote machine
        //return _is_server ? 0 : 1;
        return _is_server ? 1 : 0;
    }

    void send_blocking(const void *sbuf, std::size_t size)
    {
        CHKERR("oob_sock_ctx", sock_send(_socket_data, sbuf, size));
    }

    void recv_blocking(void *rbuf, std::size_t size)
    {
        CHKERR("oob_sock_ctx", sock_recv(_socket_data, rbuf, size));
    }

    void exchange(const void *sbuf, void *rbuf, std::size_t size)
    {
        if (_is_server) {
            recv_blocking(rbuf, size);
            send_blocking(sbuf, size);
        } else {
            send_blocking(sbuf, size);
            recv_blocking(rbuf, size);
        }
    }

    void barrier() // will work only with two ranks
    {
        uint32_t tmp = 0xdeadbeaf;
        uint32_t tmp2 = 0xdeadbeaf;
        exchange(&tmp, &tmp2, sizeof(uint32_t));
    }

    double time()
    {
        return MPI_Wtime();
    }

    bool _is_server;
    bool _reuse_server;
    const int _port;
    int _socket_data;
    int _socket_listen;
};

#endif
