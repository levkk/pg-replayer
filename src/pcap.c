/*
 * Capture packets and get the ones we care about.
 *
 * NOT USED.
 */

#include <stdio.h> //For standard things
#include <stdlib.h>  //malloc
#include <string.h>  //memset
#include <stdint.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/ip.h>  //Provides declarations for ip header
#include <netinet/tcp.h> //Provides declarations for tcp header
#include <errno.h>

#define BUFFER_SIZE 65536

#ifdef APPLE
#define iphdr ip
#endif

void parse_tcp(char *buf, int size);

void capture() {
  int sock, nrecv;
  socklen_t saddr_size;
  struct sockaddr saddr;
  char *buf = malloc(BUFFER_SIZE);

  if (!buf) {
    printf("[pcap] Could not allocate buffer.\n");
    exit(1);
  }

  sock = socket(AF_INET, SOCK_RAW, IPPROTO_TCP);

  if (sock < 0) {
    printf("Could not create sniffing socket.\n");
    strerror(errno);
    exit(1);
  }

  while (1) {
    printf("Waiting for data\n");
    nrecv = recvfrom(sock, buf, BUFFER_SIZE, 0, &saddr, &saddr_size);

    if (nrecv < 0) {
      printf("Could not receive data from socket.\n");
      exit(1);
    }
  }

  struct ip *iph = (struct ip*) buf;

  switch(iph->ip_p) {
    case 6: /* TCP */
      parse_tcp(buf, nrecv);
    default:
      break;
  }
}

void parse_tcp(char *buf, int size) {
  struct ip *iph = (struct ip*) buf;
  int iph_len = iph->ip_hl;

  char *ip_offset = buf + iph_len * 4; /* In 4 byte words */

  struct tcphdr *tcp = (struct tcphdr *) ip_offset;
  char *tcp_offset = ip_offset + tcp->th_off * 4; /* In 4 byte words */

  fwrite(tcp_offset, 250, 1, stdout);
  // printf("The data: %s\n", tcp_offset);

  // printf("source port: %u\n\n", ntohs(tcp->source));

}
