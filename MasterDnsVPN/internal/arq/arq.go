package arq

import (
	"context"
	"encoding/binary"
	"io"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	Enums "masterdnsvpn-go/internal/enums"
)

type StreamState int

const (
	StateOpen StreamState = iota
	StateHalfClosedLocal
	StateHalfClosedRemote
	StateClosing
	StateReset
	StateClosed
	StateDraining
	StateTimeWait
)

type PacketEnqueuer interface {
	PushTXPacket(priority int, packetType uint8, sequenceNum uint16, fragmentID uint8, totalFragments uint8, compressionType uint8, ttl time.Duration, payload []byte) bool
}

type terminalOwner interface {
	OnARQClosed(reason string)
}

type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
}

type DummyLogger struct{}

func (d *DummyLogger) Debugf(f string, a ...any) {}
func (d *DummyLogger) Infof(f string, a ...any)  {}
func (d *DummyLogger) Errorf(f string, a ...any) {}

type rxPayload struct {
	sn         uint16
	fragmentID uint8
	totalFrags uint8
	data       []byte
}

type Config struct {
	WindowSize                  int
	RTO                         float64
	MaxRTO                      float64
	IsVirtual                   bool
	StartPaused                 bool
	EnableControlReliability    bool
	ControlRTO                  float64
	ControlMaxRTO               float64
	ControlMaxRetries           int
	InactivityTimeout           float64
	DataPacketTTL               float64
	MaxDataRetries              int
	ControlPacketTTL            float64
	DataNackMaxGap              int
	DataNackInitialDelaySeconds float64
	DataNackRepeatSeconds       float64
	TerminalDrainTimeout        float64
	TerminalAckWaitTimeout      float64
	CompressionType             uint8
	IsClient                    bool
	InboundQueueSize            int

	// New FEC Fields
	FECDataShards   int
	FECParityShards int
}

type CloseOptions struct {
	Force          bool
	SendRST        bool
	SendCloseWrite bool
	SendCloseRead  bool
	AfterDrain     bool
	TTL            time.Duration
}

type ARQ struct {
	mu sync.RWMutex

	streamID             uint16
	sessionID            uint8
	ioReady              bool
	streamWorkersStarted bool
	enqueuer             PacketEnqueuer
	localConn            io.ReadWriteCloser
	logger               Logger
	cfg                  Config

	mtu int

	state        StreamState
	closed       bool
	closeReason  string
	lastActivity time.Time

	// Counters for blocks
	sndNxt uint16

	rxChan chan rxPayload

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	enc reedsolomon.Encoder
	
	// Receiver block assembly
	rxBlocks map[uint16]*rxBlock

	// Adaptive FEC tracking
	rxTotalShards   int
	rxMissingShards int
	rxLastBlock     uint16
	
	// Reorder Buffer (Ring Buffer)
	expectedSeq           uint16
	reorderBuf            [1024][][]byte
	reorderSeq            [1024]uint16
	reorderReady          [1024]bool
	lastExpectedSeqChange time.Time
}

type rxBlock struct {
	shards     [][]byte
	shardsMask []bool
	received   int
	decoded    bool
	createdAt  time.Time
}

func NewARQ(streamID uint16, sessionID uint8, enqueuer PacketEnqueuer, localConn io.ReadWriteCloser, mtu int, logger Logger, cfg Config) *ARQ {
	if logger == nil {
		logger = &DummyLogger{}
	}
	
	// Default FEC config if not provided
	if cfg.FECDataShards <= 0 {
		cfg.FECDataShards = 10
	}
	if cfg.FECParityShards < 0 {
		cfg.FECParityShards = 3
	}

	enc, err := reedsolomon.New(cfg.FECDataShards, cfg.FECParityShards)
	if err != nil {
		logger.Errorf("Failed to initialize RS encoder: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	a := &ARQ{
		streamID:   streamID,
		sessionID:  sessionID,
		ioReady:    !cfg.StartPaused,
		enqueuer:   enqueuer,
		localConn:  localConn,
		logger:     logger,
		cfg:        cfg,
		mtu:        mtu,
		state:      StateOpen,
		sndNxt:     1,
		rxChan:     make(chan rxPayload, 1024),
		ctx:        ctx,
		cancel:     cancel,
		enc:                   enc,
		rxBlocks:              make(map[uint16]*rxBlock),
		expectedSeq:           1,
		lastExpectedSeqChange: time.Now(),
	}
	return a
}

func (a *ARQ) Start() {
	a.wg.Add(1)
	go a.rxLoop()

	if a.ioReady {
		a.startStreamWorkers()
	}
}

func (a *ARQ) startStreamWorkers() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.streamWorkersStarted || a.localConn == nil {
		return
	}
	a.streamWorkersStarted = true

	a.wg.Add(1)
	go a.writeLoop()
}

func (a *ARQ) SetLocalConn(conn io.ReadWriteCloser) {
	a.mu.Lock()
	if a.localConn != nil {
		a.mu.Unlock()
		return
	}
	a.localConn = conn
	shouldStart := a.ctx != nil && a.ctx.Err() == nil && a.ioReady
	a.mu.Unlock()

	if shouldStart {
		a.startStreamWorkers()
	}
}

func (a *ARQ) SetIOReady(ready bool) {
	a.mu.Lock()
	changed := a.ioReady != ready
	a.ioReady = ready
	a.mu.Unlock()

	if changed && ready {
		a.startStreamWorkers()
	}
}

func (a *ARQ) Done() <-chan struct{} {
	return a.ctx.Done()
}

func (a *ARQ) IsClosed() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.closed
}

func (a *ARQ) State() StreamState {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.state
}

func (a *ARQ) HasPendingSequence(sn uint16) bool {
	return false
}

func (a *ARQ) NoteTXPacketDequeued(packetType uint8, sequenceNum uint16, fragmentID uint8) {
	// Unused in FEC
}

func (a *ARQ) Close(reason string, opts CloseOptions) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	a.closeReason = reason
	a.state = StateClosed
	a.cancel()

	if a.localConn != nil {
		a.localConn.Close()
	}
	a.mu.Unlock()

	if opts.SendRST {
		a.enqueuer.PushTXPacket(
			Enums.DefaultPacketPriority(Enums.PACKET_STREAM_RST),
			Enums.PACKET_STREAM_RST,
			0, 0, 0, 0, 0, nil,
		)
	}

	if owner, ok := a.enqueuer.(terminalOwner); ok {
		owner.OnARQClosed(reason)
	}
}

func (a *ARQ) writeLoop() {
	defer a.wg.Done()



	readChan := make(chan []byte)
	readErrChan := make(chan error, 1)

	go func() {
		for {
			tmp := make([]byte, 8192) // Read large chunks
			n, err := a.localConn.Read(tmp)
			if n > 0 {
				b := make([]byte, n)
				copy(b, tmp[:n])
				select {
				case readChan <- b:
				case <-a.ctx.Done():
					return
				}
			}
			if err != nil {
				select {
				case readErrChan <- err:
				case <-a.ctx.Done():
				}
				return
			}
		}
	}()

	var coalesceBuf []byte
	coalesceTimeout := 2 * time.Millisecond // Smart coalescing threshold
	coalesceTimer := time.NewTimer(coalesceTimeout)
	coalesceTimer.Stop()
	isCoalescing := false

	flushBlock := func() {
		for len(coalesceBuf) > 0 {

		chunkSize := a.mtu
		if len(coalesceBuf) < chunkSize {
			chunkSize = len(coalesceBuf)
		}

		numShards := (len(coalesceBuf) + chunkSize - 1) / chunkSize
		if numShards > a.cfg.FECDataShards {
			numShards = a.cfg.FECDataShards
		}
		
		// Take only the data that fits in numShards
		dataToProcess := coalesceBuf
		maxData := numShards * chunkSize
		if len(dataToProcess) > maxData {
			dataToProcess = coalesceBuf[:maxData]
			coalesceBuf = coalesceBuf[maxData:]
		} else {
			coalesceBuf = nil
		}

		// chunkSize is strictly <= a.mtu
		numShards = (len(dataToProcess) + chunkSize - 1) / chunkSize

		a.mu.Lock()
		parityShards := a.cfg.FECParityShards
		blockID := a.sndNxt
		a.sndNxt++
		a.mu.Unlock()

		// Thresholding: If it's a small packet (fits in 1 shard), send with 0 parity to use ONE resolver
		if numShards == 1 {
			parityShards = 0
		}

		totalShards := numShards + parityShards
		packedTotalFrags := (uint8(numShards) << 4) | uint8(parityShards)
		shards := make([][]byte, totalShards)

		for i := 0; i < totalShards; i++ {
			shards[i] = make([]byte, chunkSize+2)
		}

		offset := 0
		for i := 0; i < numShards; i++ {
			end := offset + chunkSize
			if end > len(dataToProcess) {
				end = len(dataToProcess)
			}
			data := dataToProcess[offset:end]
			binary.BigEndian.PutUint16(shards[i][0:2], uint16(len(data)))
			copy(shards[i][2:], data)
			offset = end
		}

		if parityShards > 0 {
			enc, err := reedsolomon.New(numShards, parityShards)
			if err == nil {
				_ = enc.Encode(shards)
			}
		}

		for i := 0; i < numShards; i++ {
			a.enqueuer.PushTXPacket(
				Enums.PacketPriorityNormal,
				Enums.PACKET_STREAM_DATA,
				blockID, uint8(i), packedTotalFrags, a.cfg.CompressionType, 0, shards[i],
			)
		}

		if parityShards > 0 {
			for i := numShards; i < totalShards; i++ {
				a.enqueuer.PushTXPacket(
					Enums.PacketPriorityNormal,
					Enums.PACKET_STREAM_FEC_PARITY,
					blockID, uint8(i), packedTotalFrags, a.cfg.CompressionType, 0, shards[i],
				)
			}
		}

		}
		
		isCoalescing = false
		coalesceTimer.Stop()
	}

	for {
		select {
		case <-a.ctx.Done():
			return
		case err := <-readErrChan:
			flushBlock()
			a.Close("local connection read error: "+err.Error(), CloseOptions{SendCloseWrite: true})
			return
		case <-coalesceTimer.C:
			flushBlock()
		case data := <-readChan:
			coalesceBuf = append(coalesceBuf, data...)
			if !isCoalescing {
				isCoalescing = true
				coalesceTimer.Reset(coalesceTimeout)
			}
			
			// Flush early if buffer gets too large
			if len(coalesceBuf) >= a.cfg.FECDataShards*a.mtu {
				if !coalesceTimer.Stop() {
					select {
					case <-coalesceTimer.C:
					default:
					}
				}
				flushBlock()
			}
		}
	}
}

func (a *ARQ) rxLoop() {
	defer a.wg.Done()

	cleanupTicker := time.NewTicker(500 * time.Millisecond)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-cleanupTicker.C:
			a.mu.Lock()
			now := time.Now()
			for sn, block := range a.rxBlocks {
				if now.Sub(block.createdAt) > 10*time.Second {
					delete(a.rxBlocks, sn)
				}
			}
			
			// Soft Disconnect for Deadlock / Head-of-Line Blocking
			hasPendingBlocks := false
			for i := 0; i < len(a.reorderReady); i++ {
				if a.reorderReady[i] {
					hasPendingBlocks = true
					break
				}
			}
			
			if hasPendingBlocks && now.Sub(a.lastExpectedSeqChange) > 2000*time.Millisecond {
				a.mu.Unlock()
				a.logger.Errorf("Reorder buffer deadlock detected. Soft disconnecting stream %d", a.streamID)
				a.Close("reorder buffer deadlock (missing block)", CloseOptions{Force: true})
				return
			}
			
			a.mu.Unlock()
		case p := <-a.rxChan:
			a.processReceivedShard(p)
		}
	}
}

func (a *ARQ) processReceivedShard(p rxPayload) {
	a.mu.Lock()
	block, exists := a.rxBlocks[p.sn]
	
	dataShards := int(p.totalFrags >> 4)
	parityShards := int(p.totalFrags & 0x0F)
	totalShards := dataShards + parityShards
	if dataShards == 0 {
		dataShards = int(p.totalFrags) - a.cfg.FECParityShards
		if dataShards < 1 {
			dataShards = 1
		}
		parityShards = int(p.totalFrags) - dataShards
		totalShards = int(p.totalFrags)
	}

	if !exists {
		if a.rxLastBlock > 0 && p.sn > a.rxLastBlock {
			diff := int(p.sn - a.rxLastBlock)
			a.rxMissingShards += diff * dataShards
			a.rxTotalShards += diff * dataShards
		}
		a.rxLastBlock = p.sn

		block = &rxBlock{
			shards:     make([][]byte, totalShards),
			shardsMask: make([]bool, totalShards),
			received:   0,
			decoded:    false,
			createdAt:  time.Now(),
		}
		a.rxBlocks[p.sn] = block
	}

	if p.fragmentID >= uint8(len(block.shards)) {
		a.mu.Unlock()
		return
	}

	if !block.shardsMask[p.fragmentID] {
		block.shards[p.fragmentID] = p.data
		block.shardsMask[p.fragmentID] = true
		block.received++
		a.rxTotalShards++

		if block.received >= dataShards && !block.decoded {
			if parityShards > 0 {
				enc, err := reedsolomon.New(dataShards, parityShards)
				if err == nil {
					_ = enc.ReconstructData(block.shards)
				}
			}

			block.decoded = true
			
			// Flush valid shards to localConn (extract before unlocking)
			var validPayloads [][]byte
			for i := 0; i < len(block.shards) && i < dataShards; i++ {
				shard := block.shards[i]
				if len(shard) < 2 {
					continue
				}
				dataLen := binary.BigEndian.Uint16(shard[0:2])
				if int(dataLen)+2 <= len(shard) {
					validData := make([]byte, dataLen)
					copy(validData, shard[2 : 2+dataLen])
					validPayloads = append(validPayloads, validData)
				}
			}
			
			diff := p.sn - a.expectedSeq
			
			// Сценарий В: Призрак (отставший пакет)
			if diff >= 32768 {
				a.mu.Unlock()
				return
			}
			
			// Сценарий Б: Обогнавший (будущий пакет)
			if diff > 0 && diff < 32768 {
				idx := p.sn % 1024
				a.reorderBuf[idx] = validPayloads
				a.reorderSeq[idx] = p.sn
				a.reorderReady[idx] = true
				a.mu.Unlock()
				return
			}
			
			// Сценарий А: Идеальный (p.sn == a.expectedSeq)
			var payloadsToFlush [][]byte
			payloadsToFlush = append(payloadsToFlush, validPayloads...)
			a.expectedSeq++
			a.lastExpectedSeqChange = time.Now()
			
			// Каскадная проверка Reorder Buffer (освобождение накопившихся блоков)
			for {
				idx := a.expectedSeq % 1024
				if a.reorderReady[idx] && a.reorderSeq[idx] == a.expectedSeq {
					payloadsToFlush = append(payloadsToFlush, a.reorderBuf[idx]...)
					a.reorderReady[idx] = false
					a.reorderBuf[idx] = nil // Очистка для GC
					a.expectedSeq++
					a.lastExpectedSeqChange = time.Now()
				} else {
					break
				}
			}
			
			// Unlock BEFORE blocking on network writes to avoid deadlocking the dispatcher
			a.mu.Unlock()
			
			for _, payload := range payloadsToFlush {
				if a.localConn != nil {
					_, _ = a.localConn.Write(payload)
				}
			}
			return // Mutex is already unlocked
		}
	}

	// Send FEC METRICS every ~200 shards received (approx 20 blocks)
	if a.rxTotalShards >= 200 {
		lossRatio := uint8((a.rxMissingShards * 100) / a.rxTotalShards)
		if lossRatio > 100 {
			lossRatio = 100
		}
		a.enqueuer.PushTXPacket(
			Enums.PacketPriorityCritical,
			Enums.PACKET_STREAM_FEC_METRICS,
			0, 0, 0, 0, 0, []byte{lossRatio},
		)
		a.rxTotalShards = 0
		a.rxMissingShards = 0
	}

	a.mu.Unlock()
}

func (a *ARQ) QueueRXPacket(packetType uint8, sequenceNum uint16, fragmentID uint8, totalFragments uint8, payload []byte) {
	if a.IsClosed() {
		return
	}
	
	switch packetType {
	case Enums.PACKET_STREAM_DATA, Enums.PACKET_STREAM_FEC_PARITY:
		select {
		case a.rxChan <- rxPayload{sn: sequenceNum, fragmentID: fragmentID, totalFrags: totalFragments, data: payload}:
		default:
		}
	case Enums.PACKET_STREAM_FEC_METRICS:
		if len(payload) >= 1 {
			lossRatio := payload[0]
			a.mu.Lock()
			// Adaptive FEC logic
			newParity := int((float64(a.cfg.FECDataShards) * float64(lossRatio)) / 100.0)
			
			// Always add a small buffer (+1) if loss > 0, cap at MaxParity (e.g. 5)
			if lossRatio > 0 {
				newParity += 1
			}
			if newParity > 5 {
				newParity = 5
			}
			if newParity < 0 {
				newParity = 0
			}
			
			if a.cfg.FECParityShards != newParity {
				a.logger.Infof("Adaptive FEC adjusted: Loss %d%% -> Parity shards %d", lossRatio, newParity)
				a.cfg.FECParityShards = newParity
				
				// Recreate encoder with new parity
				if newParity > 0 {
					enc, err := reedsolomon.New(a.cfg.FECDataShards, newParity)
					if err == nil {
						a.enc = enc
					}
				}
			}
			a.mu.Unlock()
		}
	case Enums.PACKET_STREAM_CLOSE_WRITE, Enums.PACKET_STREAM_CLOSE_READ, Enums.PACKET_SESSION_CLOSE, Enums.PACKET_STREAM_RST:
		if packetType == Enums.PACKET_STREAM_RST {
			a.Close("Received RST", CloseOptions{Force: true})
		} else {
			a.Close("Received remote close", CloseOptions{Force: true})
		}
	}
}

func (a *ARQ) SendControlPacketWithTTL(packetType uint8, sequenceNum uint16, fragmentID uint8, totalFragments uint8, payload []byte, priority int, trackForAck bool, customAckType *uint8, ttl time.Duration) bool {
	return a.enqueuer.PushTXPacket(
		priority,
		packetType,
		sequenceNum,
		fragmentID,
		totalFragments,
		0,
		ttl,
		payload,
	)
}

func (a *ARQ) ReceiveData(sn uint16, data []byte) bool {
	a.QueueRXPacket(Enums.PACKET_STREAM_DATA, sn, 0, 1, data)
	return true
}

func (a *ARQ) HandleDataNack(sn uint16) bool {
	return false
}

func (a *ARQ) MarkCloseReadReceived() {
	a.Close("Remote read closed", CloseOptions{Force: true})
}

func (a *ARQ) MarkCloseWriteReceived() {
	a.Close("Remote write closed", CloseOptions{Force: true})
}

func (a *ARQ) MarkRstReceived() {
	a.Close("RST received", CloseOptions{Force: true})
}

func (a *ARQ) HandleAckPacket(packetType uint8, sn uint16, fragID uint8) bool {
	return false
}

func (a *ARQ) IsReset() bool {
	return a.IsClosed()
}
