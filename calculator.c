#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

// log_printf - макрос для отладочного вывода, добавляющий время с первого использования, имя функции и номер строки
const char* log_prefix(const char* func, int line) {
    struct timespec spec; clock_gettime(CLOCK_REALTIME, &spec); long long current_msec = spec.tv_sec * 1000L + spec.tv_nsec / 1000000;
    static _Atomic long long start_msec_storage = -1; long long start_msec = -1; if (atomic_compare_exchange_strong(&start_msec_storage, &start_msec, current_msec)) start_msec = current_msec;
    long long delta_msec = current_msec - start_msec; const int max_func_len = 13;
    static __thread char prefix[100]; sprintf(prefix, "%lld.%03lld %*s():%d    ", delta_msec / 1000, delta_msec % 1000, max_func_len, func, line); sprintf(prefix + max_func_len + 13, "[tid=%ld]", syscall(__NR_gettid));
    return prefix;
}
#define log_printf_impl(fmt, ...) { time_t t = time(0); dprintf(2, "%s: " fmt "%s", log_prefix(__FUNCTION__, __LINE__), __VA_ARGS__); }
// Format: <time_since_start> <func_name>:<line> : <custom_message>
#define log_printf(...) log_printf_impl(__VA_ARGS__, "")

#define conditional_handle_error(stmt, msg) \
    do { if (stmt) { perror(msg " (" #stmt ")"); exit(EXIT_FAILURE); } } while (0)


double EPS = 1e-10;

double f(double x) {
  if (fabs(x) < EPS) {
    return 0.;
  }
  return 1. / sqrt(x);
}

const size_t NUM_OF_PARTS = 1000;

double calc(double from, double to) {
  double result = 0.;
  double delta = (to - from) / NUM_OF_PARTS;
  for (double left = from, right = left + delta; left < to; left = right, right += delta) {
    result += (f(right) + f(left)) * delta / 2.;
  }
  return result;
}

struct Message {
  double from;
  double to;
};

void handle_conn(int conn_fd) {
  struct Message msg;
  read(conn_fd, &msg, sizeof(msg));
  sleep(5);
  double result = calc(msg.from, msg.to);
  write(conn_fd, &result, sizeof(result));
}

void handle_cycle(int socket_fd) {
  while (true) {
    struct sockaddr_in peer_addr = {0};
    socklen_t peer_addr_size = sizeof(struct sockaddr_in);
    int connection_fd = accept(socket_fd, (struct sockaddr*)&peer_addr, &peer_addr_size);
    conditional_handle_error(connection_fd == -1, "can't accept incoming connection");
    log_printf("Server accepted connection and start reading\n");
    
    handle_conn(connection_fd);

    shutdown(connection_fd, SHUT_RDWR); 
    close(connection_fd);
  }
}


int main(int argc, char** argv) {
  log_printf("Server started\n");

  conditional_handle_error(argc < 2, "Don't know the port");
  int port = atoi(argv[1]);

  int socket_fd = socket(AF_INET, SOCK_STREAM, 0); 
  conditional_handle_error(socket_fd == -1, "can't initialize socket");

  int reuse_val = 1;
  setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &reuse_val, sizeof(reuse_val));
  setsockopt(socket_fd, SOL_SOCKET, SO_REUSEPORT, &reuse_val, sizeof(reuse_val));

  struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons(port), .sin_addr = 0};
  int bind_ret = bind(socket_fd, (struct sockaddr*)&addr, sizeof(addr));
  conditional_handle_error(bind_ret == -1, "can't bind to unix socket");
  log_printf("Socket is bound\n");

  int listen_ret = listen(socket_fd, SOMAXCONN);
  conditional_handle_error(listen_ret == -1, "can't listen to unix socket");
  log_printf("Listening started\n");

  handle_cycle(socket_fd);

  shutdown(socket_fd, SHUT_RDWR); 
  close(socket_fd);

  log_printf("Calculator finished\n");
  return 0;
}
