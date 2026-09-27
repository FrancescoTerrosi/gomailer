package mailer

// Wire tests for the staged (hold-the-dot) delivery against a scripted
// SMTPS server. The invariant under test is the one the design rests on:
// the terminating dot is the delivery act — everything else (envelope,
// DATA, the whole message) may sit at the provider before the fire time,
// but the dot must not leave the wire until Commit, and the error contract
// must tell "the dot never left" (safe to fall back cold) from "the dot
// left, the outcome is unknown" (terminal, verify manually).

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// shortBudgets shortens every per-command deadline for one test and
// restores them afterwards (the budgets are package vars precisely so
// the wire tests do not wait production timeouts).
func shortBudgets(t *testing.T, d time.Duration) {
	t.Helper()
	oldCmd, oldDot, oldAns, oldDial := commandBudget, dotBudget, answerBudget, dialBudget
	commandBudget, dotBudget, answerBudget, dialBudget = d, d, d, d
	t.Cleanup(func() {
		commandBudget, dotBudget, answerBudget, dialBudget = oldCmd, oldDot, oldAns, oldDial
	})
}

// fakePECServer is a minimal scripted SMTPS server: just enough ESMTP
// (EHLO/AUTH/MAIL/RCPT/DATA/NOOP/QUIT) for the client, recording what the
// DATA phase received and the instant the terminating dot arrived. Its
// behavior at the dot is scripted per test:
//
//	"answer"      — reply 250 (the normal path)
//	"hang"        — never answer: the client's 250 read must time out
//	"close-data"  — vanish right after the 354, mid-upload
//	"hang-hello"  — accept the connection and send nothing at all
//	"bad-rcpt"    — refuse the FIRST RCPT, accept later ones: a staging
//	                failure over a still-alive connection (the cold retry
//	                then succeeds, so the session's fate is the client's)
type fakePECServer struct {
	mu        sync.Mutex
	behavior  string
	dotAt     time.Time // zero until the terminating dot arrived
	data      []byte    // message bytes received before the dot
	conns     map[net.Conn]struct{}
	listener  net.Listener
	tlsConf   *tls.Config // for the STARTTLS upgrade
	t         *testing.T
	closeOnce sync.Once
	rcptSeen  bool // "bad-rcpt": the first RCPT was refused already
}

func startFakePEC(t *testing.T, behavior string) *fakePECServer {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	// The fake listens in PLAIN TCP and upgrades on STARTTLS: the client
	// dials an arbitrary port, so it takes the STARTTLS path of connect()
	// — exactly like -port 587 against the real provider.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
	s := &fakePECServer{
		behavior: behavior,
		conns:    map[net.Conn]struct{}{},
		listener: ln,
		tlsConf:  &tls.Config{Certificates: []tls.Certificate{cert}},
		t:        t,
	}
	t.Cleanup(s.stop)
	go s.accept()
	return s
}

func (s *fakePECServer) stop() {
	s.closeOnce.Do(func() { s.listener.Close() })
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
}

// kill aborts every live connection with a reset (SO_LINGER 0), the way
// a crashed provider would: a subsequent write on the client side fails
// instead of silently buffering.
func (s *fakePECServer) kill() {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		c.Close()
	}
}

// firstRcpt consumes the "refuse once" token of the "bad-rcpt" behavior:
// true exactly for the first RCPT the server ever sees.
func (s *fakePECServer) firstRcpt() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rcptSeen {
		return false
	}
	s.rcptSeen = true
	return true
}

// snapshot copies the recorded state for assertions.
func (s *fakePECServer) snapshot() (dotAt time.Time, dataLen int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dotAt, len(s.data)
}

func (s *fakePECServer) addr() string {
	return s.listener.Addr().String()
}

// config builds a client configuration pointing at the fake server. The
// port is arbitrary (not "465"), so the client takes the STARTTLS path —
// the fake upgrades on the STARTTLS command, like a real submission
// server on 587.
func (s *fakePECServer) config() MailConfig {
	_, port, _ := net.SplitHostPort(s.addr())
	return MailConfig{
		Hostname:           "127.0.0.1",
		Port:               port,
		Username:           "sender@pec.test",
		Password:           "secret",
		InsecureSkipVerify: true, // local testing only, exactly as documented
	}
}

func (s *fakePECServer) accept() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go func() {
			defer func() {
				conn.Close()
				s.mu.Lock()
				delete(s.conns, conn)
				s.mu.Unlock()
			}()
			s.handle(conn)
		}()
	}
}

func (s *fakePECServer) handle(conn net.Conn) {
	s.mu.Lock()
	behavior := s.behavior
	s.mu.Unlock()
	if behavior == "hang-hello" {
		return // nothing at all: the greeting read must time out
	}
	// active is the connection the conversation runs over: reassigned
	// to the TLS layer when the client upgrades with STARTTLS.
	var active net.Conn = conn
	r := bufio.NewReader(conn)
	line := func(str string) {
		active.Write([]byte(str + "\r\n")) // errors surface as read failures
	}
	line("220 fake ESMTP")
	for {
		cmd, err := r.ReadString('\n')
		if err != nil {
			return
		}
		switch {
		case strings.HasPrefix(strings.ToUpper(cmd), "EHLO"):
			line("250-fake says hello")
			line("250 AUTH PLAIN")
		case strings.HasPrefix(strings.ToUpper(cmd), "AUTH"):
			line("235 ok")
		case strings.HasPrefix(strings.ToUpper(cmd), "MAIL"):
			line("250 ok")
		case strings.HasPrefix(strings.ToUpper(cmd), "RCPT"):
			if behavior == "bad-rcpt" && s.firstRcpt() {
				line("550 no such recipient")
			} else {
				line("250 ok")
			}
		case strings.HasPrefix(strings.ToUpper(cmd), "NOOP"):
			if behavior == "bad-noop" {
				line("500 no") // a probe failure on a LIVE connection: SendOn must discard and reconnect
			} else {
				line("250 ok")
			}
		case strings.HasPrefix(strings.ToUpper(cmd), "STARTTLS"):
			line("220 ready to start TLS")
			tlsConn := tls.Server(conn, s.tlsConf)
			if err := tlsConn.Handshake(); err != nil {
				return
			}
			active = tlsConn
			r = bufio.NewReader(tlsConn)
		case strings.HasPrefix(strings.ToUpper(cmd), "DATA"):
			line("354 go")
			if behavior == "close-data" {
				// Vanish mid-upload: the client's staged write fails —
				// a pre-dot failure by construction.
				if tc, ok := conn.(*net.TCPConn); ok {
					_ = tc.SetLinger(0)
				}
				conn.Close()
				return
			}
			s.readData(r, line, behavior)
			return
		case strings.HasPrefix(strings.ToUpper(cmd), "QUIT"):
			line("221 bye")
			return
		default:
			line("500 unrecognized")
		}
	}
}

// readData consumes the DATA stream line by line until the terminating
// dot, records its arrival instant, and answers per the script. It reads
// and writes through whatever connection `line`'s closure captured (the
// TLS layer after a STARTTLS upgrade).
func (s *fakePECServer) readData(r *bufio.Reader, line func(string), behavior string) {
	for {
		ln, err := r.ReadString('\n')
		if err != nil {
			return
		}
		if ln == ".\r\n" {
			s.mu.Lock()
			s.dotAt = time.Now()
			s.mu.Unlock()
			if behavior == "hang" {
				// The answer never comes — and neither does a close: keep
				// the connection open (draining any further input) so the
				// client's answer READ times out instead of seeing EOF.
				buf := make([]byte, 256)
				for {
					if _, err := r.Read(buf); err != nil {
						return // test cleanup closed the connection
					}
				}
			}
			line("250 ok")
			// Await QUIT (Commit sends it after the 250).
			for {
				ln, err = r.ReadString('\n')
				if err != nil {
					return
				}
				if strings.HasPrefix(strings.ToUpper(ln), "QUIT") {
					line("221 bye")
					return
				}
			}
		}
		s.mu.Lock()
		s.data = append(s.data, ln...)
		s.mu.Unlock()
	}
}

// stageFixture warms a session against the fake and prepares a small
// certified message on it.
func stageFixture(t *testing.T, cfg MailConfig) (*Session, []byte, MailContent) {
	t.Helper()
	sess, err := cfg.WarmUp()
	if err != nil {
		t.Fatalf("WarmUp: %v", err)
	}
	content := MailContent{From: cfg.Username, To: []string{"dest@pec.test"}, Subject: "held", Body: "one dot short"}
	msg, err := cfg.Prepare(&content)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	return sess, msg, content
}

// TestStagedHoldsThenCommits: after StageOn returns, the provider holds
// the whole message but NOT the dot; Commit then puts the dot on the wire
// within the commit instant — and the earlier flush means the hold left
// the wire idle (all message bytes arrived before it began).
func TestStagedHoldsThenCommits(t *testing.T) {
	srv := startFakePEC(t, "answer")
	cfg := srv.config()
	sess, msg, content := stageFixture(t, cfg)

	staged, err := cfg.StageOn(sess, msg, content.To)
	if err != nil {
		t.Fatalf("StageOn: %v", err)
	}

	// The hold: everything but the dot is at the provider.
	time.Sleep(150 * time.Millisecond)
	dotAt, dataLen := srv.snapshot()
	if !dotAt.IsZero() {
		t.Fatal("the terminating dot left the wire before Commit — prefire")
	}
	if dataLen != len(msg) {
		t.Fatalf("staged bytes = %d, want %d (the flush must push the bufio tail before the hold)", dataLen, len(msg))
	}

	// Commit: the dot is the delivery act and lands now.
	commitStart := time.Now()
	if err := staged.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	dotAt, _ = srv.snapshot()
	if dotAt.IsZero() {
		t.Fatal("Commit did not write the dot")
	}
	if d := dotAt.Sub(commitStart); d < 0 || d > time.Second {
		t.Fatalf("dot landed %v after Commit started, want ~instant", d)
	}
}

// TestCommitAnswerLostIsTerminal: the dot left, the provider never
// answered — the outcome is uncertain, so the error must wrap ErrDotOut
// (terminal ambiguity: never retried, only verified) and must take about
// the (shortened) answer budget, not hang forever.
func TestCommitAnswerLostIsTerminal(t *testing.T) {
	shortBudgets(t, 300*time.Millisecond)
	srv := startFakePEC(t, "hang")
	cfg := srv.config()
	sess, msg, content := stageFixture(t, cfg)

	staged, err := cfg.StageOn(sess, msg, content.To)
	if err != nil {
		t.Fatalf("StageOn: %v", err)
	}
	start := time.Now()
	err = staged.Commit()
	if err == nil {
		t.Fatal("a hung provider must fail the commit")
	}
	if !errors.Is(err, ErrDotOut) {
		t.Fatalf("error after the dot must wrap ErrDotOut (terminal ambiguity), got: %v", err)
	}
	if d := time.Since(start); d < 250*time.Millisecond {
		t.Fatalf("commit gave up after %v; the answer budget did not bound it", d)
	}
	if dotAt, _ := srv.snapshot(); dotAt.IsZero() {
		t.Fatal("the dot must have left the wire (the provider received it)")
	}
}

// TestHeldSessionKilledDuringHoldFallsBackSafe: the provider resets the
// connection during the hold; the dot write fails and NOTHING left the
// wire as a complete line — the error must NOT wrap ErrDotOut, because
// the caller ("just go") is allowed one cold full send at the fire time.
func TestHeldSessionKilledDuringHoldFallsBackSafe(t *testing.T) {
	srv := startFakePEC(t, "answer")
	cfg := srv.config()
	sess, msg, content := stageFixture(t, cfg)

	staged, err := cfg.StageOn(sess, msg, content.To)
	if err != nil {
		t.Fatalf("StageOn: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // the hold idles
	srv.kill()                         // the provider dies mid-hold

	err = staged.Commit()
	if err == nil {
		t.Fatal("commit against a dead provider must fail")
	}
	if errors.Is(err, ErrDotOut) {
		t.Fatalf("the dot never left the wire, but the error claims terminal ambiguity: %v", err)
	}
	if dotAt, _ := srv.snapshot(); !dotAt.IsZero() {
		t.Fatal("no dot must have been recorded by the (dead) server")
	}
}

// TestStageUploadFailureIsPreDot: the provider vanishing mid-upload is a
// staging failure — pre-dot by construction, never ErrDotOut.
//
// The message is deliberately FAT (≫ any kernel socket buffer): the
// flush of a small message can complete into local buffers before the
// reset is processed — a TCP race the test must not depend on — while a
// fat one cannot, so the upload failure is guaranteed by two
// independent mechanisms (the reset arriving mid-write, and the
// buffers filling against a peer that no longer reads).
func TestStageUploadFailureIsPreDot(t *testing.T) {
	srv := startFakePEC(t, "close-data")
	cfg := srv.config()
	sess, _, content := stageFixture(t, cfg)

	content.Body = strings.Repeat("upload payload line\r\n", 8*65536) // ≈ 9.4 MB
	fat, err := cfg.Prepare(&content)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	if _, err := cfg.StageOn(sess, fat, content.To); err == nil {
		t.Fatal("an upload to a vanished provider must fail")
	} else if errors.Is(err, ErrDotOut) {
		t.Fatalf("staging failure must be pre-dot, got: %v", err)
	}
}

// TestStageFailureConsumesSession: ANY staging failure must close the
// session (see StageOn). The refusal arrives over a still-alive
// connection, so the next command WOULD run if the session were left
// open — and a leftover session with a half-open envelope, or an
// abandoned dot-writer, turns the next command into a possible early
// commit (textproto auto-closes a pending dot-writer). The NOOP failing
// here proves the failure itself consumed the session — and the "just
// go" retry then works cold, over a fresh connection.
func TestStageFailureConsumesSession(t *testing.T) {
	srv := startFakePEC(t, "bad-rcpt")
	cfg := srv.config()
	sess, msg, content := stageFixture(t, cfg)

	if _, err := cfg.StageOn(sess, msg, content.To); err == nil {
		t.Fatal("staging against a recipient-refusing provider must fail")
	} else if errors.Is(err, ErrDotOut) {
		t.Fatalf("a staging failure is pre-dot by construction, got: %v", err)
	}
	// The session must be dead: no command may ever run on it again.
	if err := sess.client.Noop(); err == nil {
		t.Fatal("the failed staging left a live session — StageOn must consume it (the next command could auto-write the dot)")
	}
	// "Just go": the retry is a cold full send over a FRESH session —
	// the consumed one is worthless by construction.
	if err := cfg.SendOn(sess, msg, content.To); err != nil {
		t.Fatalf("SendOn must retry cold past the consumed session: %v", err)
	}
	dotAt, dataLen := srv.snapshot()
	if dotAt.IsZero() {
		t.Fatal("the cold retry never delivered (no dot at the server)")
	}
	if dataLen != len(msg) {
		t.Fatalf("delivered bytes = %d, want %d", dataLen, len(msg))
	}
}

// TestColdSendOnComposesStageCommit: SendOn (the cold path and the
// "just go" fallback) is StageOn+Commit back to back — the fake server
// sees exactly one dot and a clean transaction.
func TestColdSendOnComposesStageCommit(t *testing.T) {
	srv := startFakePEC(t, "answer")
	cfg := srv.config()
	_, msg, content := stageFixture(t, cfg) // discards its own session; SendOn dials fresh

	if err := cfg.SendOn(nil, msg, content.To); err != nil {
		t.Fatalf("SendOn: %v", err)
	}
	dotAt, dataLen := srv.snapshot()
	if dotAt.IsZero() {
		t.Fatal("SendOn never delivered (no dot at the server)")
	}
	if dataLen != len(msg) {
		t.Fatalf("delivered bytes = %d, want %d", dataLen, len(msg))
	}
}

// TestSendOnReconnectsOnFailedProbe: a warm session whose NOOP probe
// fails on a LIVE connection (the provider grew unhappy) is discarded —
// SendOn opens a fresh one and delivers over it: the probe runs BEFORE
// any delivery command, so nothing was lost.
func TestSendOnReconnectsOnFailedProbe(t *testing.T) {
	srv := startFakePEC(t, "bad-noop")
	cfg := srv.config()
	sess, msg, content := stageFixture(t, cfg)

	if err := cfg.SendOn(sess, msg, content.To); err != nil {
		t.Fatalf("SendOn must reconnect past a failed probe and deliver: %v", err)
	}
	dotAt, _ := srv.snapshot()
	if dotAt.IsZero() {
		t.Fatal("the reconnected session never delivered (no dot at the server)")
	}
}

// TestSendOnDeadServerFailsCleanly: a provider that is gone entirely —
// connections reset, listener dead — fails SendOn before anything is
// delivered: never ErrDotOut, nothing on the wire.
func TestSendOnDeadServerFailsCleanly(t *testing.T) {
	srv := startFakePEC(t, "answer")
	cfg := srv.config()
	sess, msg, content := stageFixture(t, cfg)

	srv.kill() // the warm session dies
	srv.stop() // ...and so does the listener: even a reconnect has nowhere to go
	err := cfg.SendOn(sess, msg, content.To)
	if err == nil {
		t.Fatal("delivery against a dead provider must fail")
	}
	if errors.Is(err, ErrDotOut) {
		t.Fatalf("nothing left the wire, but the error claims terminal ambiguity: %v", err)
	}
	if dotAt, _ := srv.snapshot(); !dotAt.IsZero() {
		t.Fatal("nothing must have been delivered")
	}
}

// TestHungServerTimesOutWarmUp: per-command deadlines (§4.5.3.2) bound
// every exchange — a server that accepts TCP and then goes silent fails
// the warmup within the (shortened) budget instead of hanging the
// runner (the stop-hang corner).
func TestHungServerTimesOutWarmUp(t *testing.T) {
	shortBudgets(t, 300*time.Millisecond)
	srv := startFakePEC(t, "hang-hello")
	cfg := srv.config()

	start := time.Now()
	if _, err := cfg.WarmUp(); err == nil {
		t.Fatal("a server that never greets must fail the warmup")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("warmup against a silent server took %v; the per-command deadline did not bound it", d)
	}
}
