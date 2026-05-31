package udpserver

import (
	"errors"
	"fmt"
	"strings"
	"time"

	DnsParser "masterdnsvpn-go/internal/dnsparser"
	domainMatcher "masterdnsvpn-go/internal/domainmatcher"
	Enums "masterdnsvpn-go/internal/enums"
	"masterdnsvpn-go/internal/logger"
	"masterdnsvpn-go/internal/security"
	VpnProto "masterdnsvpn-go/internal/vpnproto"
)

func (s *Server) handlePacket(packet []byte) []byte {
	if s.log != nil && s.log.Enabled(logger.LevelDebug) {
		s.log.Debugf("Received packet of size %d", len(packet))
	}
	parsed, err := DnsParser.ParseDNSRequestLite(packet)
	if err != nil {
		if errors.Is(err, DnsParser.ErrNotDNSRequest) || errors.Is(err, DnsParser.ErrPacketTooShort) {
			if s.log != nil && s.log.Enabled(logger.LevelDebug) {
				s.log.Debugf("Dropping packet, Not DNS or too short: %v", err)
			}
			return nil
		}

		return s.buildNoDataResponseLogged(packet, "request-parse-failed")
	}

	if !parsed.HasQuestion {
		return s.buildNoDataResponseLogged(packet, "request-has-no-question")
	}

	decision := s.domainMatcher.Match(parsed)
	if decision.Action == domainMatcher.ActionProcess {
		response := s.handleTunnelCandidate(packet, parsed, decision)
		if response != nil {
			return response
		}

		return s.buildNoDataResponseLiteLogged(packet, parsed, "domain-match-process-failed")
	}

	if decision.Action == domainMatcher.ActionFormatError || decision.Action == domainMatcher.ActionNoData {
		if s.log != nil {
			s.log.Debugf("Domain match rejected: reason=%s, reqName=%s, base=%s, labels=%s, qtype=%d", decision.Reason, decision.RequestName, decision.BaseDomain, decision.Labels, decision.QuestionType)
		}
		return s.buildNoDataResponseLiteLogged(packet, parsed, "domain-match-no-data")
	}

	return s.buildNoDataResponseLiteLogged(packet, parsed, "domain-match-no-data")
}

func extractClientEmail(requestName string, baseDomain string) string {
	if !strings.HasSuffix(requestName, baseDomain) {
		return ""
	}
	prefix := requestName[:len(requestName)-len(baseDomain)]
	if len(prefix) > 0 && prefix[len(prefix)-1] == '.' {
		prefix = prefix[:len(prefix)-1]
	}
	if prefix == "" {
		return ""
	}
	parts := strings.Split(prefix, ".")
	return parts[len(parts)-1]
}

func localStripLabelDots(labels string) string {
	if strings.IndexByte(labels, '.') == -1 {
		return labels
	}
	var b strings.Builder
	b.Grow(len(labels))
	for i := 0; i < len(labels); i++ {
		if labels[i] != '.' {
			b.WriteByte(labels[i])
		}
	}
	return b.String()
}

func extractEncLabels(requestName string, email string, baseDomain string) string {
	suffixToRemove := "." + email + "." + baseDomain
	var encLabels string
	if strings.HasSuffix(requestName, suffixToRemove) {
		encLabels = requestName[:len(requestName)-len(suffixToRemove)]
	} else if strings.HasSuffix(requestName, email+"."+baseDomain) {
		encLabels = ""
	} else {
		encLabels = requestName
	}
	return localStripLabelDots(encLabels)
}

func (s *Server) handleTunnelCandidate(packet []byte, parsed DnsParser.LitePacket, decision domainMatcher.Decision) []byte {
	// 1. Dynamic authentication via SQLite
	email := extractClientEmail(decision.RequestName, decision.BaseDomain)
	var activeCodec *security.Codec = s.codec
	var targetLabels string = decision.Labels

	if email != "" {
		clientKey, err := getClientKeyAndCheckStatus(s.cfg.SQLitePath, email)
		if err != nil {
			if s.log != nil {
				s.log.Warnf("❌ <red>Authentication failed for client email: %s, error: %v</red>", email, err)
			}
			return s.buildNoDataResponseLiteLogged(packet, parsed, "unauthorized-client")
		}
		
		// Create personal codec for the client
		clientCodec, err := security.NewCodecFromConfig(s.cfg, clientKey)
		if err != nil {
			if s.log != nil {
				s.log.Errorf("❌ <red>Failed to create codec for client key, error: %v</red>", err)
			}
			return s.buildNoDataResponseLiteLogged(packet, parsed, "codec-creation-failed")
		}
		activeCodec = clientCodec
		targetLabels = extractEncLabels(decision.RequestName, email, decision.BaseDomain)
	}

	vpnPacket, err := VpnProto.ParseInflatedFromLabels(targetLabels, activeCodec)
	if err != nil {
		return s.buildNoDataResponseLiteLogged(packet, parsed, "vpn-proto-parse-failed")
	}

	if vpnPacket.PacketType == Enums.PACKET_SESSION_CLOSE {
		s.handleSessionCloseNotice(vpnPacket, time.Now())
		return s.buildNoDataResponseLiteLogged(packet, parsed, "session-close-notice")
	}

	if !isPreSessionRequestType(vpnPacket.PacketType) {
		validation := s.validatePostSessionPacket(packet, decision.RequestName, vpnPacket)
		if !validation.ok {
			return validation.response
		}

		if !s.handlePostSessionPacket(vpnPacket, validation.record) {
			return s.buildNoDataResponseLiteLogged(packet, parsed, fmt.Sprintf("post-session-unhandled-%s", Enums.PacketTypeName(vpnPacket.PacketType)))
		}

		if s.log != nil {
			s.log.Infof("Pre-session parsed: %s | Stream: %d | Seq: %d | Frag: %d/%d", Enums.PacketTypeName(vpnPacket.PacketType), vpnPacket.StreamID, vpnPacket.SequenceNum, vpnPacket.FragmentID, vpnPacket.TotalFragments)
		}
		return s.serveQueuedOrPong(packet, decision.RequestName, validation.record, time.Now())
	}

	switch vpnPacket.PacketType {
	case Enums.PACKET_MTU_UP_REQ:
		return s.handleMTUUpRequest(packet, parsed, decision, vpnPacket)
	case Enums.PACKET_MTU_DOWN_REQ:
		return s.handleMTUDownRequest(packet, parsed, decision, vpnPacket)
	case Enums.PACKET_SESSION_INIT:
		return s.handleSessionInitRequest(packet, decision, vpnPacket)
	default:
		return s.buildNoDataResponseLiteLogged(packet, parsed, fmt.Sprintf("pre-session-unhandled-%s", Enums.PacketTypeName(vpnPacket.PacketType)))
	}
}
