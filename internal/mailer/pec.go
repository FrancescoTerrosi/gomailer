package mailer

// PEC (Posta Elettronica Certificata) compliance.
//
// The sender-side requirements implemented here follow the Italian legal and
// technical framework for certified email:
//
//   - DPR 11 febbraio 2005, n. 68 (the enabling regulation);
//   - DM 2 novembre 2005, "Regole tecniche" (the technical rules), as
//     subsequently amended by AgID (Agenzia per l'Italia Digitale);
//   - RFC 6109 "La Posta Elettronica Certificata" (informational mapping).
//
// A PEC message is distinguished from ordinary email by a set of extra
// RFC 5322 headers carried in the transport envelope (the "busta di
// trasporto") plus the mandatory use of TLS on the wire. The headers below
// are what a compliant sender must emit.

// X-Trasporto is the single discriminator header: a value of
// "posta-certificata" marks the message as certified mail. Without it a PEC
// MTA treats the message as ordinary (non-certified) email.
const (
	HeaderTrasporto = "X-Trasporto"
	TrasportoPEC    = "posta-certificata"
)

// X-Riferimento-Message-ID references the Message-ID of a previous certified
// message. It MUST be present when the current message is a reply to a PEC,
// and MUST be absent otherwise.
const HeaderRiferimentoMessageID = "X-Riferimento-Message-ID"

// X-TipoRicevuta selects the kind of receipt (ricevuta) the sender asks the
// PEC system to generate. X-Ricevuta is set by the receiver when it actually
// produces the receipt.
const (
	HeaderTipoRicevuta = "X-TipoRicevuta"
	HeaderRicevuta     = "X-Ricevuta"
)

// TipoRicevuta selects the type of receipt requested from the PEC system.
type TipoRicevuta string

const (
	// RicevutaCompleta asks for a receipt that embeds the full original
	// message. This is the default and most commonly used form.
	RicevutaCompleta TipoRicevuta = "completa"
	// RicevutaBreve asks for a receipt that carries the original headers plus
	// a digest of the message.
	RicevutaBreve TipoRicevuta = "breve"
	// RicevutaSintetica asks for a receipt that carries only the essential
	// delivery data (no message content).
	RicevutaSintetica TipoRicevuta = "sintetica"
)
