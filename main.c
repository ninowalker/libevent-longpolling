#include <stdlib.h>
#include <assert.h>
#include <signal.h>
#include <sys/queue.h>
#include <string.h>
#include <getopt.h>

#include <event.h>
#include <event2/http.h>

#ifndef TAILQ_FIRST
#define TAILQ_FIRST(head)	((head)->tqh_first)
#endif /* !TAILQ_FIRST */

#ifndef TAILQ_EMPTY
#define TAILQ_EMPTY(head)	((head)->tqh_first == NULL)
#endif /* !TAILQ_EMPTY */

struct event_base *base;
struct evhttp *server;
int verbose = 0;

struct message {
  char *type;

  struct evbuffer *content;

  TAILQ_ENTRY(message) next;
};

struct topic {
  const char *name;

  TAILQ_HEAD(,message) messages;

  struct evhttp_request *pending;

  TAILQ_ENTRY(topic) next;
};

TAILQ_HEAD(,topic) topics;

static struct topic *_topic_lookup(const char *name) {
  struct topic *topic;

  for (topic = TAILQ_FIRST(&topics); topic; topic = topic->next.tqe_next) {
    if (strcmp(name, topic->name) == 0) {
      if (verbose)
        fprintf(stderr, "matching topic '%s' has been found\n", name);

      return topic;
    }
  }

  topic = calloc(1, sizeof *topic);
  assert(topic != NULL);

  topic->name = strdup(name);
  assert(topic->name);

  TAILQ_INIT(&topic->messages);

  TAILQ_INSERT_TAIL(&topics, topic, next);

  if (verbose)
    fprintf(stderr, "topic '%s' has been created\n", name);

  return topic;
}

static unsigned int _topic_flush(struct topic *topic) {
  unsigned int count = 0;

  struct message *msg;
  while ((msg = TAILQ_FIRST(&topic->messages)) != NULL) {
    TAILQ_REMOVE(&topic->messages,msg,next);

    evbuffer_free(msg->content);
    if (msg->type)
      free(msg->type);

    free(msg);

    count += 1;
  }

  TAILQ_INIT(&topic->messages);
  topic->pending = NULL;

  return count;
}

struct event *hup_event;
static void _flush_queues(int fd, short evt, void *arg) {
  assert(arg == NULL);

  if (verbose)
    fprintf(stderr, "SIGHUP received, flushing queues... \n");

  struct topic *topic;

  for (topic = TAILQ_FIRST(&topics); topic; topic = topic->next.tqe_next) {
    unsigned int count;

    count = _topic_flush(topic);

    if (verbose)
      fprintf(stderr, "flushed topic '%s', which was containing %zd message(s)\n", topic->name, count);
  }
}

static void _consumer_pull(struct evhttp_request *req, void *arg) {
  assert(arg == NULL);

  const char *name;
  char *consumer = strstr(evhttp_request_get_uri(req), "/consumer");
  assert(consumer != NULL);
  name = &consumer[9];

  if (verbose)
    fprintf(stderr, "consuming request on '%s'\n", name);

  struct topic *topic = _topic_lookup(name);
  assert(topic != NULL);

  if (TAILQ_EMPTY(&topic->messages)) {
    if (verbose)
      fprintf(stderr, "but topic is empty, put request on hold\n");

    evhttp_request_own(req);
    topic->pending = req;
  }
  else {
    struct message *msg = TAILQ_FIRST(&topic->messages);
    assert(msg != NULL);

    TAILQ_REMOVE(&topic->messages,msg,next);

    if (verbose)
      fprintf(stderr, "preparing message of type '%s', %d bytes to be sent\n",
        msg->type ? msg->type : "text/plain" , EVBUFFER_LENGTH(msg->content));

    struct evkeyvalq *headers = evhttp_request_get_output_headers(req);
    assert(headers != NULL);

    evhttp_add_header(headers, "Content-Type",
      (msg->type ? msg->type : "text/plain"));
    evhttp_send_reply(req, 200, "OK", msg->content);

    if (verbose)
      fprintf(stderr, "response embedding notification of topic '%s' sent\n",
        name);

    evbuffer_free(msg->content);
    if (msg->type)
      free(msg->type);
    free(msg);
  }
}

static void _producer_push(struct evhttp_request *req, void *arg) {
  assert(arg == NULL);

  struct evbuffer *body = evhttp_request_get_input_buffer(req);
  assert(body != NULL);

  const char *name;
  char *producer = strstr(evhttp_request_get_uri(req), "/producer");
  assert(producer != NULL);
  name = &producer[9];

  if (verbose)
    fprintf(stderr, "producing request on '%s'\n", name);

  struct topic *topic = _topic_lookup(name);
  assert(topic != NULL);

  struct evkeyvalq *headers = evhttp_request_get_input_headers(req);
  assert(headers != NULL);

  const char *type = evhttp_find_header(headers, "Content-Type");
  assert(type != NULL);

  if (topic->pending) {
    if (verbose)
      fprintf(stderr, "a pending consumer request was ongoing\n");

    struct evkeyvalq *headers = evhttp_request_get_output_headers(topic->pending);
    assert(headers != NULL);

    evhttp_add_header(headers, "Content-Type", type);
    evhttp_send_reply(topic->pending, 200, "OK", body);

    if (verbose)
      fprintf(stderr, "response embedding notification of topic '%s' sent\n",
        name);

    topic->pending = NULL;
  }
  else {
    if (verbose)
      fprintf(stderr, "queueing a message of type '%s', %d bytes\n",
        type, EVBUFFER_LENGTH(body));

    struct message *msg = calloc(1, sizeof(*msg));
    assert(msg != NULL);

    msg->type = strdup(type);
    assert(msg->type != NULL);

    msg->content = evbuffer_new();
    assert(msg->content != NULL);

    evbuffer_add_printf(msg->content, "%.*s", EVBUFFER_LENGTH(body),
      EVBUFFER_DATA(body));

    TAILQ_INSERT_TAIL(&topic->messages,msg,next);
  }

  if (verbose)
    fprintf(stderr, "response to producing request done\n");

  evhttp_send_reply(req, 200, "OK", NULL);
}

static void _purge(struct evhttp_request *req, void *arg) {
  assert(arg == NULL);

  const char *name;
  char *purge = strstr(evhttp_request_get_uri(req), "/purge");
  assert(purge != NULL);
  name = &purge[6];

  struct topic *topic;
  unsigned int count = 0;
  for (topic = TAILQ_FIRST(&topics); topic; topic = topic->next.tqe_next) {
    if (strcmp(name, topic->name) == 0)
      count = _topic_flush(topic);
  }

  evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", "text/plain");

  struct evbuffer *buf = evbuffer_new();
  evbuffer_add_printf(buf, "%zd", count);
  evhttp_send_reply(req, 200, "OK", buf);
  evbuffer_free(buf);
}

static void _gencb(struct evhttp_request *req, void *arg) {
  assert(arg == NULL);

  const char *ruri = evhttp_request_get_uri(req);

  if (strncmp(ruri, "/consumer", 9) == 0) {
    _consumer_pull(req, NULL);
  }
  else if (strncmp(ruri, "/producer", 9) == 0) {
    _producer_push(req, NULL);
  }
  else if (strncmp(ruri, "/purge", 6) == 0) {
    _purge(req, NULL);
  }
  else {
    if (verbose)
      fprintf(stderr, "unrecognized request URI '%s', sending 404\n", ruri);

    evhttp_send_error(req, HTTP_BADREQUEST, "Bad Request");
  }
}

int main(int argc, char **argv) {
  static struct option options[] = {
    {"verbose", 0, 0, 'v'},
    {"help", 0, 0, 'h'},
    {"address", 0, 0, 'a'},
    {"port", 0, 0, 'p'},
    {0, 0, 0, 0}
  };

  base = event_base_new();
  assert(base != NULL);

  server = evhttp_new(base);
  assert(server != NULL);

  char *address = "0.0.0.0";
  int port = 8888;

  while (1) {
    int c;

    c = getopt_long(argc, argv, "vha:p:", options, NULL);
    if (c == -1)
      break;

    switch (c) {
    case 'v':
      verbose = 1;
      break;
    case 'a':
      address = optarg;
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 'h':
    default:
      fprintf(stderr, "usage: %s [-v|--verbose] [-a|--address a.b.c.d] [-p|--port num]\n",
        argv[0]);
      exit(1);
    }
  }

  int ret;
  ret = evhttp_bind_socket(server, address, port);
  assert(ret == 0);

  if (verbose)
    fprintf(stderr, "server bound to %s:%d\n", address, port);

  hup_event = evsignal_new(base, SIGHUP, _flush_queues, NULL);
  assert(hup_event != NULL);

  event_add(hup_event, NULL);

  if (verbose)
    fprintf(stderr, "SIGHUP bound to flush\n");

  evhttp_set_gencb(server, _gencb, NULL);

  TAILQ_INIT(&topics);

  if (verbose)
    fprintf(stderr, "entering dispatching loop, ready\n");

  event_base_dispatch(base);

  return 0;
}
