#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <errno.h>
#include <time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <string.h>
#include <stdatomic.h>
#include <stdbool.h>
    

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

int min(int x, int y) {
  return x < y ? x : y;
}

double minf(double x, double y) {
  return x < y ? x : y;
}

const int PORT = 31010;
const int LISTEN_BACKLOG = 2;

struct Message {
  double from;
  double to;
};

bool send_calc_part(double from, double to, int socket_fd) {
  if (socket_fd < 0) { return false; }
  struct Message msg = {.from = from, .to = to};
  bool res = write(socket_fd, &msg, sizeof(msg)) > 0;
  if (!res) { log_printf("Failed to send part to calculate\n"); }
  return res;
}

bool try_get_result(int fd, double* res_ptr) {
  if (fd < 0) { return false; }
  int size = read(fd, res_ptr, sizeof(double));
  bool res = size > 0;
  if (!res) { log_printf("Failed to get result of calculation\n"); }
  return res;
}

int create_socket(int port) {
  int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
  conditional_handle_error(socket_fd == -1, "can't initialize socket");

  int flags = 1;
  int set_opt = setsockopt(socket_fd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
  conditional_handle_error(set_opt, "Can't set SO_KEEPALIVE\n");

  flags = 10;
  set_opt = setsockopt(socket_fd, SOL_TCP, TCP_KEEPIDLE, (void *)&flags, sizeof(flags));
  conditional_handle_error(set_opt, "Can't set TCP_KEEPIDLE\n");

  flags = 5;
  set_opt = setsockopt(socket_fd, SOL_TCP, TCP_KEEPINTVL, (void *)&flags, sizeof(flags));
  conditional_handle_error(set_opt, "Can't set TCP_KEEPINTVL\n");

  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  struct hostent* hosts = gethostbyname("localhost");
  conditional_handle_error(!hosts, "can't get host by name\n");
  memcpy(&addr.sin_addr, hosts->h_addr_list[0], sizeof(addr.sin_addr));

  int connect_ret = connect(socket_fd, (struct sockaddr*)&addr, sizeof(addr));

  if (connect_ret < 0) {
    log_printf("Failed to connect to socket\n");
    return -1;
  }

  return socket_fd;
}

void destroy_socket(int socket_fd) {
  if (socket_fd < 0) { return; }
  shutdown(socket_fd, SHUT_RDWR); 
  close(socket_fd);
}

const int MAX_NODES = 100;
const int MAX_PARTS = 1000;

int get_nodes_num(int argc, char** argv) {
  if (argc < 2) {
    return 1;
  }
  return min(atoi(argv[1]), MAX_NODES);
}

const double FROM = 0.;
const double TO = 1.;

const int PORT_START = 31010;


int main(int argc, char** argv) {
  log_printf("Master started\n");

  int n = get_nodes_num(argc, argv);
  int parts = 2 * n;
  double delta = (TO - FROM) / parts;

  double sum = 0.;

  int sock_fd[MAX_NODES];
  int sock_part[MAX_NODES];
  bool calculated_part[MAX_PARTS];
  for (int i = 0; i < parts; ++i) {
    calculated_part[i] = false;
  }

  bool calculated = false;
  for (int try = 0; try < 2 * parts; ++try) {
    for (int calc_num = 0, part = 0; calc_num < n; ++calc_num) {
      for (; calculated_part[part] && part < parts; ++part) {}
      if (part >= parts) {
        if (calc_num == 0) { calculated = true; }
        break;
      }

      double from = FROM + delta * part;
      double to = from + delta;

      log_printf("Calc part ");
      printf("from %f to %f\n", from, to);

      int port = PORT_START + calc_num;
      sock_fd[calc_num] = create_socket(port);
      if (sock_fd[calc_num] < 0) {
        log_printf("Failed to create a socket\n");
        continue;
      }
      if (!send_calc_part(from, to, sock_fd[calc_num])) {
        destroy_socket(sock_fd[calc_num]);
        sock_fd[calc_num] = -1;
        log_printf("Failed to send a message to calculate the part\n");
        continue;
      }
      sock_part[calc_num] = part;
      ++part;
    }

    if (calculated) { break; }

    for (int calc_num = 0; calc_num < n; ++calc_num) {
      if (sock_fd[calc_num] < 0) { continue; }
      double res = 0.;
      int part = sock_part[calc_num];
      calculated_part[part] = try_get_result(sock_fd[calc_num], &res);
      if (calculated_part[part]) {
        log_printf("Calculated successful: ");
        printf("part %d - %f\n", part, res);
        sum += res;
      }
      destroy_socket(sock_fd[calc_num]);
      sock_fd[calc_num] = -1;
    }

    if (calculated) { break; }

    sleep(3);
  }

  conditional_handle_error(!calculated, "Can't calculate: all nodes crashed");
  printf("%f\n", sum);
  log_printf("Master finished\n");
  return 0;
}
