#include <stdlib.h>
#include <assert.h>
#include <signal.h>
#include <sys/queue.h>
#include <string.h>

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
    if (strcmp(name, topic->name) == 0)
      return topic;
  }

  topic = calloc(1, sizeof *topic);
  assert(topic != NULL);

  topic->name = strdup(name);
  assert(topic->name);

  TAILQ_INIT(&topic->messages);

  TAILQ_INSERT_TAIL(&topics, topic, next);

  return topic;
}

static void _topic_flush(struct topic *topic) {
  struct message *msg;
  while ((msg = TAILQ_FIRST(&topic->messages)) != NULL) {
    TAILQ_REMOVE(&topic->messages,msg,next);

    evbuffer_free(msg->content);
    if (msg->type)
      free(msg->type);
    free(msg);
  }

  TAILQ_INIT(&topic->messages);
  topic->pending = NULL;
}

struct event *hup_event;
static void _flush_queues(int fd, short evt, void *arg) {
  assert(arg == NULL);

  printf("SIGHUP received, flushing queues... \n");
  fflush(stdout);

  struct topic *topic;

  for (topic = TAILQ_FIRST(&topics); topic; topic = topic->next.tqe_next) {
    _topic_flush(topic);
    printf("flushed topic '%s'\n", topic->name);
  }

  printf("done\n");
}

static void _consumer_pull(struct evhttp_request *req, void *arg) {
  assert(arg == NULL);

  printf("consumer pulling ");
  fflush(stdout);

  const char *name;
  char *consumer = strstr(evhttp_request_get_uri(req), "/consumer");
  assert(consumer != NULL);
  name = &consumer[9];

  struct topic *topic = _topic_lookup(name);
  assert(topic != NULL);

  if (TAILQ_EMPTY(&topic->messages)) {
    printf("but empty message queue, waiting for input...\n");

    evhttp_request_own(req);
    topic->pending = req;
  }
  else {
    struct message *msg = TAILQ_FIRST(&topic->messages);
    assert(msg != NULL);

    TAILQ_REMOVE(&topic->messages,msg,next);

    printf(", popped %d bytes from queue\n", EVBUFFER_LENGTH(msg->content));

    struct evkeyvalq *headers = evhttp_request_get_output_headers(req);
    assert(headers != NULL);

    evhttp_add_header(headers, "Content-Type",
      (msg->type ? msg->type : "text/plain"));
    evhttp_send_reply(req, 200, "OK", msg->content);

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

  struct topic *topic = _topic_lookup(name);
  assert(topic != NULL);

  struct evkeyvalq *headers = evhttp_request_get_input_headers(req);
  assert(headers != NULL);

  const char *type = evhttp_find_header(headers, "Content-Type");
  assert(type != NULL);

  if (topic->pending) {
    struct evkeyvalq *headers = evhttp_request_get_output_headers(topic->pending);
    assert(headers != NULL);

    evhttp_add_header(headers, "Content-Type", type);
    evhttp_send_reply(topic->pending, 200, "OK", body);

    topic->pending = NULL;
  }
  else {
    printf("pushing %d bytes of data (type=%s)\n", EVBUFFER_LENGTH(body),
      type);

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

  evhttp_send_reply(req, 200, "OK", NULL);
}

static void _purge(struct evhttp_request *req, void *arg) {
  assert(arg == NULL);

  const char *name;
  char *purge = strstr(evhttp_request_get_uri(req), "/purge");
  assert(purge != NULL);
  name = &purge[6];

  struct topic *topic;
  for (topic = TAILQ_FIRST(&topics); topic; topic = topic->next.tqe_next) {
    if (strcmp(name, topic->name) == 0)
      _topic_flush(topic);
  }

  evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Length", "0");
  evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", "text/plain");

  evhttp_send_reply(req, 200, "OK", NULL);
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
    fprintf(stderr, "unrecognized request URI '%s'\n", ruri);

    abort();
  }
}

int main(int argc, char **argv) {
  base = event_base_new();
  assert(base != NULL);

  server = evhttp_new(base);
  assert(server != NULL);

  int ret;
  ret = evhttp_bind_socket(server, "0.0.0.0", 8888);
  assert(ret == 0);

  hup_event = evsignal_new(base, SIGHUP, _flush_queues, NULL);
  assert(hup_event != NULL);

  event_add(hup_event, NULL);

  evhttp_set_gencb(server, _gencb, NULL);

  TAILQ_INIT(&topics);

  event_base_dispatch(base);

  return 0;
}
