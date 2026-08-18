// ai-generated: multi-tier bootstrap dns resolver (DoT RFC 7858, UDP/TCP DNS) for whitelist environments

package common

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type bootstrapCacheEntry struct {
	ips     []net.IP
	expires time.Time
}

var (
	bootstrapCache sync.Map

	dotEndpoints = []struct {
		address string
		sni     string
	}{
		{address: "77.88.8.8:853", sni: "common.dot.dns.yandex.net"},
		{address: "77.88.8.1:853", sni: "common.dot.dns.yandex.net"},
		{address: "1.1.1.1:853", sni: "cloudflare-dns.com"},
		{address: "8.8.8.8:853", sni: "dns.google"},
	}

	plainDNSEndpoints = []string{
		"77.88.8.8:53",
		"77.88.8.1:53",
	}
)

// ResolveBootstrapDomain resolves critical domains (api.vk.me, login.vk.ru, calls.okcdn.ru)
// directly via DoT (port 853) and plain DNS (port 53) using fixed IP addresses, bypassing
// uninitialized or blocked system/tunnel DNS in cellular whitelist environments.
func ResolveBootstrapDomain(ctx context.Context, dialer N.Dialer, domain string) ([]net.IP, error) {
	domain = strings.TrimSpace(domain)
	if domain == "" {
		return nil, errors.New("empty domain")
	}
	if ip := net.ParseIP(domain); ip != nil {
		return []net.IP{ip}, nil
	}
	if val, ok := bootstrapCache.Load(domain); ok {
		entry := val.(bootstrapCacheEntry)
		if time.Now().Before(entry.expires) {
			return entry.ips, nil
		}
		bootstrapCache.Delete(domain)
	}

	query, err := buildDNSQuery(domain, 1) // Type A (IPv4)
	if err != nil {
		return nil, err
	}

	var lastErr error

	// 1. Try DoT (RFC 7858) over port 853 to Yandex (and fallback to Cloudflare/Google)
	for _, ep := range dotEndpoints {
		resp, queryErr := queryDoTEndpoint(ctx, dialer, ep.address, ep.sni, query)
		if queryErr != nil {
			lastErr = queryErr
			continue
		}
		ips, parseErr := parseDNSResponse(resp)
		if parseErr != nil {
			lastErr = parseErr
			continue
		}
		if len(ips) > 0 {
			bootstrapCache.Store(domain, bootstrapCacheEntry{
				ips:     ips,
				expires: time.Now().Add(10 * time.Minute),
			})
			return ips, nil
		}
	}

	// 2. Fallback to direct UDP/TCP DNS (port 53) to Yandex DNS
	for _, ep := range plainDNSEndpoints {
		resp, queryErr := queryPlainDNSEndpoint(ctx, dialer, ep, query)
		if queryErr != nil {
			lastErr = queryErr
			continue
		}
		ips, parseErr := parseDNSResponse(resp)
		if parseErr != nil {
			lastErr = parseErr
			continue
		}
		if len(ips) > 0 {
			bootstrapCache.Store(domain, bootstrapCacheEntry{
				ips:     ips,
				expires: time.Now().Add(10 * time.Minute),
			})
			return ips, nil
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("bootstrap dns failed for %s: %w", domain, lastErr)
	}
	return nil, fmt.Errorf("bootstrap dns failed for %s: no response", domain)
}

func buildDNSQuery(domain string, qtype uint16) ([]byte, error) {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint16(buf[0:2], 0x1234) // Transaction ID
	binary.BigEndian.PutUint16(buf[2:4], 0x0100) // Standard query, recursion desired
	binary.BigEndian.PutUint16(buf[4:6], 1)      // QDCOUNT = 1

	cleanDomain := strings.TrimSuffix(domain, ".")
	parts := strings.Split(cleanDomain, ".")
	for _, part := range parts {
		if len(part) == 0 || len(part) > 63 {
			return nil, errors.New("invalid domain label")
		}
		buf = append(buf, byte(len(part)))
		buf = append(buf, part...)
	}
	buf = append(buf, 0) // Null byte terminating QNAME

	qtypeBytes := make([]byte, 4)
	binary.BigEndian.PutUint16(qtypeBytes[0:2], qtype) // QTYPE (1 for A)
	binary.BigEndian.PutUint16(qtypeBytes[2:4], 1)     // QCLASS (1 for IN)
	buf = append(buf, qtypeBytes...)
	return buf, nil
}

func parseDNSResponse(data []byte) ([]net.IP, error) {
	if len(data) < 12 {
		return nil, errors.New("dns response too short")
	}
	flags := binary.BigEndian.Uint16(data[2:4])
	rcode := flags & 0x000F
	if rcode != 0 {
		return nil, fmt.Errorf("dns response rcode %d", rcode)
	}
	qdcount := int(binary.BigEndian.Uint16(data[4:6]))
	ancount := int(binary.BigEndian.Uint16(data[6:8]))
	offset := 12

	// Skip Question section
	for i := 0; i < qdcount; i++ {
		var err error
		offset, err = skipDNSName(data, offset)
		if err != nil {
			return nil, err
		}
		offset += 4 // QTYPE (2) + QCLASS (2)
		if offset > len(data) {
			return nil, errors.New("truncated question section")
		}
	}

	// Parse Answer section
	var ips []net.IP
	for i := 0; i < ancount; i++ {
		if offset >= len(data) {
			break
		}
		var err error
		offset, err = skipDNSName(data, offset)
		if err != nil {
			return nil, err
		}
		if offset+10 > len(data) {
			return nil, errors.New("truncated answer header")
		}
		rtype := binary.BigEndian.Uint16(data[offset : offset+2])
		rdlength := int(binary.BigEndian.Uint16(data[offset+8 : offset+10]))
		offset += 10
		if offset+rdlength > len(data) {
			return nil, errors.New("truncated rdata")
		}
		if rtype == 1 && rdlength == 4 { // Type A (IPv4)
			ip := make(net.IP, 4)
			copy(ip, data[offset:offset+4])
			ips = append(ips, ip)
		} else if rtype == 28 && rdlength == 16 { // Type AAAA (IPv6)
			ip := make(net.IP, 16)
			copy(ip, data[offset:offset+16])
			ips = append(ips, ip)
		}
		offset += rdlength
	}
	if len(ips) == 0 {
		return nil, errors.New("no ip addresses in dns response")
	}
	return ips, nil
}

func skipDNSName(data []byte, offset int) (int, error) {
	for offset < len(data) {
		length := int(data[offset])
		if length == 0 {
			return offset + 1, nil
		}
		if length&0xc0 == 0xc0 {
			if offset+2 > len(data) {
				return 0, errors.New("truncated compression pointer")
			}
			return offset + 2, nil
		}
		offset += 1 + length
	}
	return 0, errors.New("unterminated name")
}

func queryDoTEndpoint(ctx context.Context, dialer N.Dialer, endpointHostPort string, sni string, query []byte) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(reqCtx, "tcp", M.ParseSocksaddr(endpointHostPort))
	if err != nil {
		return nil, fmt.Errorf("dial dot %s: %w", endpointHostPort, err)
	}
	defer conn.Close()

	tlsConn := tls.Client(conn, &tls.Config{
		ServerName: sni,
		MinVersion: tls.VersionTLS12,
	})
	if err := tlsConn.HandshakeContext(reqCtx); err != nil {
		return nil, fmt.Errorf("tls handshake %s: %w", sni, err)
	}

	frame := make([]byte, 2+len(query))
	binary.BigEndian.PutUint16(frame[0:2], uint16(len(query)))
	copy(frame[2:], query)

	if _, err := tlsConn.Write(frame); err != nil {
		return nil, fmt.Errorf("write dot request: %w", err)
	}

	lenBuf := make([]byte, 2)
	if _, err := io.ReadFull(tlsConn, lenBuf); err != nil {
		return nil, fmt.Errorf("read dot length: %w", err)
	}
	respLen := int(binary.BigEndian.Uint16(lenBuf))
	if respLen < 12 || respLen > 4096 {
		return nil, fmt.Errorf("invalid dot response length %d", respLen)
	}

	respBuf := make([]byte, respLen)
	if _, err := io.ReadFull(tlsConn, respBuf); err != nil {
		return nil, fmt.Errorf("read dot body: %w", err)
	}
	return respBuf, nil
}

func queryPlainDNSEndpoint(ctx context.Context, dialer N.Dialer, endpointHostPort string, query []byte) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// Try UDP first
	udpConn, err := dialer.DialContext(reqCtx, "udp", M.ParseSocksaddr(endpointHostPort))
	if err == nil {
		defer udpConn.Close()
		if _, err := udpConn.Write(query); err == nil {
			buf := make([]byte, 2048)
			if n, err := udpConn.Read(buf); err == nil && n >= 12 {
				return buf[:n], nil
			}
		}
	}

	// Try TCP fallback
	tcpConn, err := dialer.DialContext(reqCtx, "tcp", M.ParseSocksaddr(endpointHostPort))
	if err != nil {
		return nil, err
	}
	defer tcpConn.Close()

	frame := make([]byte, 2+len(query))
	binary.BigEndian.PutUint16(frame[0:2], uint16(len(query)))
	copy(frame[2:], query)

	if _, err := tcpConn.Write(frame); err != nil {
		return nil, err
	}

	lenBuf := make([]byte, 2)
	if _, err := io.ReadFull(tcpConn, lenBuf); err != nil {
		return nil, err
	}
	respLen := int(binary.BigEndian.Uint16(lenBuf))
	if respLen < 12 || respLen > 4096 {
		return nil, fmt.Errorf("invalid dns response length %d", respLen)
	}

	respBuf := make([]byte, respLen)
	if _, err := io.ReadFull(tcpConn, respBuf); err != nil {
		return nil, err
	}
	return respBuf, nil
}
