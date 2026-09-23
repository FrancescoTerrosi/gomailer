package main

import (
	"testing"
	"time"
)

func TestParseFireTimeRelative(t *testing.T) {
	got, err := parseFireTime("", "90s")
	if err != nil {
		t.Fatalf("parseFireTime: %v", err)
	}
	if d := time.Until(got); d < 80*time.Second || d > 90*time.Second {
		t.Fatalf("relative fire time off: %v", d)
	}
}

func TestParseFireTimeRFC3339(t *testing.T) {
	want := "2026-07-04T15:00:00+02:00"
	got, err := parseFireTime(want, "")
	if err != nil {
		t.Fatalf("parseFireTime: %v", err)
	}
	if got.Format(time.RFC3339) != want {
		t.Fatalf("got %s, want %s", got.Format(time.RFC3339), want)
	}
}

func TestParseFireTimeLocalDate(t *testing.T) {
	at := time.Now().Add(48 * time.Hour).Format("2006-01-02 15:04:05")
	got, err := parseFireTime(at, "")
	if err != nil {
		t.Fatalf("parseFireTime: %v", err)
	}
	if diff := got.Sub(time.Now().Add(48 * time.Hour)); diff < -time.Minute || diff > time.Minute {
		t.Fatalf("date form off by %v", diff)
	}
}

// TestParseFireTimeClockToday pins the regression: a bare "15:04:05" must
// resolve to TODAY (not year 0) at that local clock time.
func TestParseFireTimeClockToday(t *testing.T) {
	want := time.Now().Add(time.Hour)
	at := want.Format("15:04:05")
	got, err := parseFireTime(at, "")
	if err != nil {
		t.Fatalf("parseFireTime: %v", err)
	}
	if got.Year() < 2020 {
		t.Fatalf("clock form resolved to bogus year %d (year-zero bug)", got.Year())
	}
	if got.Format("2006-01-02") != want.Format("2006-01-02") {
		t.Fatalf("clock form must resolve to today, got %s want %s",
			got.Format("2006-01-02"), want.Format("2006-01-02"))
	}
	if got.Hour() != want.Hour() || got.Minute() != want.Minute() {
		t.Fatalf("clock mismatch: got %s want %s",
			got.Format("15:04:05"), want.Format("15:04:05"))
	}
}

func TestParseFireTimeClockRollsToTomorrow(t *testing.T) {
	// One hour AGO, expressed as bare clock time → must roll to tomorrow.
	past := time.Now().Add(-time.Hour)
	at := past.Format("15:04:05")
	got, err := parseFireTime(at, "")
	if err != nil {
		t.Fatalf("parseFireTime: %v", err)
	}
	// ~24h later than the past instant (23-25h window absorbs DST shifts).
	if d := got.Sub(past); d < 23*time.Hour || d > 25*time.Hour {
		t.Fatalf("roll-to-tomorrow off: %v", d)
	}
}

func TestParseFireTimeRejectsGarbage(t *testing.T) {
	if _, err := parseFireTime("garbage", ""); err == nil {
		t.Fatal("accepted garbage -at")
	}
	if _, err := parseFireTime("", ""); err == nil {
		t.Fatal("accepted empty fire time")
	}
}
