package sql

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"strconv"
)

type CostModel interface {
	Init()
	Type() string
	DistributeCost() uint64
	CalculateCost() uint64
	ResultCost() uint64
	WorkArgs() physicalplan.HashJoinWorkArgs
}

var planPRPD = settings.RegisterBoolSetting(
	"sql.distsql.hashjoin_prpd.enabled",
	"if set we plan interleaved table joins instead of merge joins when possible",
	false,
)

var planPnR = settings.RegisterBoolSetting(
	"sql.distsql.hashjoin_pnr.enabled",
	"if set we plan interleaved table joins instead of merge joins when possible",
	false,
)

var planBaseHash = settings.RegisterBoolSetting(
	"sql.distsql.hashjoin_bashash.enabled",
	"if set we plan interleaved table joins instead of merge joins when possible",
	false,
)

func tableReaderRowCount(tableReader *execinfrapb.TableReaderSpec) uint64 {
	var size, rangeCount uint64
	pendingSpans := make([]execinfrapb.TableReaderSpan, 0)
	for _, span := range tableReader.Spans {
		count, ok := spanKeyCount(span.Span.Key, span.Span.EndKey, true)
		if ok {
			size = size + count
			rangeCount = rangeCount + uint64(len(span.Ranges))
			continue
		}

		pendingSpans = append(pendingSpans, span)
	}
	if rangeCount == 0 {
		return 0
	}

	// deal pending spans
	rangeAvgSize := size / rangeCount
	for _, span := range pendingSpans {
		var spanSize uint64
		for _, r := range span.Ranges {
			count, ok := spanKeyCount(r.StartKey, r.EndKey, true)
			if ok {
				spanSize += count
				continue
			}
			spanSize += rangeAvgSize
		}
		size += spanSize
	}

	return size
}

// calculate TableReaderSpan contains key counts.
// fixedKey indicate startKey/endKey has prefix.
func spanKeyCount(startKey, endKey []byte, fixedKey bool) (uint64, bool) {
	const KeyLen = 2

	if fixedKey && (len(startKey) <= KeyLen || len(endKey) <= KeyLen) {
		return 0, false
	}

	fixKey := func (key []byte) []byte {
		length := len(key)
		if length >= 8 {
			key = key[length - 8 : length]
		} else {
			zeroByte := []byte{0, 0, 0, 0, 0, 0, 0, 0}
			copy(zeroByte[8-length:], key)
			key = zeroByte
		}
		return key
	}
	startKey = fixKey(startKey)
	endKey = fixKey(endKey)

	var start, end uint64
	bufferStart := bytes.NewBuffer(startKey)
	bufferEnd := bytes.NewBuffer(endKey)
	binary.Read(bufferStart, binary.BigEndian, &start)
	binary.Read(bufferEnd, binary.BigEndian, &end)

	return end - start, true
}

type distJoinHelper struct {
	// estimate row counts
	leftEstimateRowCounts  uint64
	rightEstimateRowCounts uint64
	// skew data
	leftHeavyHitters 		[]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter
	rightHeavyHitters 	[]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter
	// left and right routers
	processors 					[]physicalplan.Processor
	leftRouters 				[]physicalplan.ProcessorIdx
	rightRouters 				[]physicalplan.ProcessorIdx
	// join node's ID
	joinNodes						[]roachpb.NodeID
	// getway's ID
	gatewayID 					roachpb.NodeID
	frequencyThreshold  float64

	// left/right all size
	allLeftSize 			uint64
	allRightSize 			uint64
	// skew size of every left or right router
	leftRouterSkewSize    []uint64
	rightRouterSkewSize   []uint64
	// skew data in map, intersect
	leftSkewData  *util.FastIntMap
	rightSkewData *util.FastIntMap
	intersect 	  *util.FastIntSet
}

func (djh *distJoinHelper) init() {
	if djh.leftRouterSkewSize != nil && djh.rightRouterSkewSize != nil {
		return
	}

	// estimate every router' skew data size
	calOneSide := func (routers []physicalplan.ProcessorIdx, heavyHitters []execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, estimateRowCounts uint64) ([]uint64, uint64) {
		routerRowCounts := make([]uint64, len(routers))
		var skewRowCounts uint64
		for _, item := range heavyHitters {
			skewRowCounts += uint64(item.Frequency)
		}
		var allRowCounts uint64
		for i, pIdx := range routers {
			proc := djh.processors[pIdx]
			tr := proc.Spec.Core.TableReader
			routerRowCounts[i] = tableReaderRowCount(tr)
			allRowCounts += routerRowCounts[i]
		}
		if allRowCounts == 0 {
			if estimateRowCounts == 0 {
				return routerRowCounts, 0
			}
			// use estimateRowCounts estimate row counts of each router (avgRowCountsPerRange * rangeCountsOfRouter)
			allRowCounts = estimateRowCounts
			var allrangeCounts uint64
			var rangeCounts []uint64
			for _, pIdx := range routers {
				proc := djh.processors[pIdx]
				tr := proc.Spec.Core.TableReader
				counts := uint64(0)
				for _, span := range tr.Spans {
					counts += uint64(len(span.Ranges))
				}
				allrangeCounts += counts
				rangeCounts = append(rangeCounts, counts)
			}
			if allrangeCounts == 0 {
				return routerRowCounts, 0
			}

			rowCountsPerRange := allRowCounts / allrangeCounts
			for i, _ := range routers {
				routerRowCounts[i] = rowCountsPerRange * rangeCounts[i]
			}
		}

		for i := range routerRowCounts {
			routerRowCounts[i] = (routerRowCounts[i] * skewRowCounts) / allRowCounts;
		}
		return routerRowCounts, allRowCounts
	}

	djh.leftRouterSkewSize, djh.allLeftSize = calOneSide(djh.leftRouters, djh.leftHeavyHitters, djh.leftEstimateRowCounts)
	djh.rightRouterSkewSize, djh.allRightSize = calOneSide(djh.rightRouters, djh.rightHeavyHitters, djh.rightEstimateRowCounts)

	djh.leftSkewData = &util.FastIntMap{}
	djh.rightSkewData = &util.FastIntMap{}
	djh.intersect = &util.FastIntSet{}
	// add left/right to map, and get intersect
	for _, item := range djh.leftHeavyHitters {
		djh.leftSkewData.Set(int(item.Value), int(item.Frequency))
	}
	for _, item := range djh.rightHeavyHitters {
		djh.rightSkewData.Set(int(item.Value), int(item.Frequency))
		_, ok := djh.leftSkewData.Get(int(item.Value))
		if ok {
			djh.intersect.Add(int(item.Value))
		}
	}
}

type PRPD struct {
	joinInfo *distJoinHelper

	intersectToLeft		 	bool
	finalLeftSkewData  	[]int64
	finalRightSkewData 	[]int64
	finalLeftSkewSize 	int64
	finalRightSkewSize 	int64

	nodeLeftSkewSize 				[]uint64
	nodeRightSkewSize				[]uint64
	materializeResultCounts []uint64
}

func(p *PRPD) Init() {
	p.joinInfo.init()

	intersect := p.joinInfo.intersect
	// get left/right final skew data, two final skew data is not intersect
	leftSkewSize := uint64(0)
	for _, item := range p.joinInfo.leftHeavyHitters {
		leftSkewSize += uint64(item.Frequency)
		if intersect.Contains(int(item.Value)) {
			continue
		}
		p.finalLeftSkewData = append(p.finalLeftSkewData, item.Value)
	}
	rightSkewSize := uint64(0)
	for _, item := range p.joinInfo.rightHeavyHitters {
		rightSkewSize += uint64(item.Frequency)
		if intersect.Contains(int(item.Value)) {
			continue
		}
		p.finalRightSkewData = append(p.finalRightSkewData, item.Value)
	}

	// intersect -> left or right
	if p.joinInfo.allLeftSize * leftSkewSize >= p.joinInfo.allRightSize * rightSkewSize {
		addIntersectToLeft := func (value int) {
			p.finalLeftSkewData = append(p.finalLeftSkewData, int64(value))
		}
		p.joinInfo.intersect.ForEach(addIntersectToLeft)
		p.intersectToLeft = true
	} else {
		addIntersectToRight := func (value int) {
			p.finalRightSkewData = append(p.finalRightSkewData, int64(value))
		}
		p.joinInfo.intersect.ForEach(addIntersectToRight)
	}

	// all right skew size
	for _, value := range p.finalRightSkewData {
		count, _ := p.joinInfo.rightSkewData.Get(int(value))
		p.finalRightSkewSize += int64(count)
	}
	// all left skew size
	for _, value := range p.finalLeftSkewData {
		count, _ := p.joinInfo.leftSkewData.Get(int(value))
		p.finalLeftSkewSize += int64(count)
	}
}

func(p *PRPD) Type() string {
	return "PRPD"
}

func(p *PRPD) DistributeCost() uint64 {
	var cost int64
	joinNodeNums := len(p.joinInfo.joinNodes)
	leftSkewMap, rightSkewMap := p.joinInfo.leftSkewData, p.joinInfo.rightSkewData

	// left mirror
	for _, value := range p.finalRightSkewData {
		count, ok := leftSkewMap.Get(int(value))
		if ok {
			cost += int64(count * (joinNodeNums - 1))
			continue
		}
		cost += int64(p.joinInfo.frequencyThreshold * float64(p.joinInfo.allLeftSize)) * int64(joinNodeNums - 1)
	}

	// right mirror
	for _, value := range p.finalLeftSkewData {
		count, ok := rightSkewMap.Get(int(value))
		if ok {
			cost += int64(count * (joinNodeNums - 1))
			continue
		}
		cost += int64(p.joinInfo.frequencyThreshold * float64(p.joinInfo.allRightSize)) * int64(joinNodeNums - 1)
	}

	return uint64(cost)
}

func(p *PRPD) CalculateCost() uint64 {
	joinNodeNums := len(p.joinInfo.joinNodes)

	// estimate left/right local row counts on every node
	p.nodeLeftSkewSize = make([]uint64, joinNodeNums)
	p.nodeRightSkewSize = make([]uint64, joinNodeNums)
	// nodeID -> bucketIdx
	bucketIdxMap := make(map[roachpb.NodeID]int)
	for i, nodeID := range p.joinInfo.joinNodes {
		bucketIdxMap[nodeID] = i
	}
	// add left/right local count
	var allLeftSkewSize uint64
	for i, pIdx := range p.joinInfo.leftRouters {
		proc := p.joinInfo.processors[pIdx]
		nodeID := proc.Node
		bucketIdx := bucketIdxMap[nodeID]
		p.nodeLeftSkewSize[bucketIdx] += p.joinInfo.leftRouterSkewSize[i]
		allLeftSkewSize += p.joinInfo.leftRouterSkewSize[i]
	}
	var allRightSkewSize uint64
	for i, pIdx := range p.joinInfo.rightRouters {
		proc := p.joinInfo.processors[pIdx]
		nodeID := proc.Node
		bucketIdx := bucketIdxMap[nodeID]
		p.nodeRightSkewSize[bucketIdx] += p.joinInfo.rightRouterSkewSize[i]
		allRightSkewSize += p.joinInfo.rightRouterSkewSize[i]
	}

	// suppose that each node has all skew data of other side.
	// left/right mirror size
	leftMirrorCount := uint64(0)
	for _, hv := range p.finalRightSkewData {
		count, ok := p.joinInfo.leftSkewData.Get(int(hv))
		if ok {
			leftMirrorCount += uint64(count)
			continue;
		}
		leftMirrorCount += uint64(p.joinInfo.frequencyThreshold * float64(p.joinInfo.allLeftSize))
	}
	rightMirrorCount := uint64(0)
	for _, hv := range p.finalLeftSkewData {
		count, ok := p.joinInfo.rightSkewData.Get(int(hv))
		if ok {
			rightMirrorCount += uint64(count)
			continue;
		}
		rightMirrorCount += uint64(p.joinInfo.frequencyThreshold * float64(p.joinInfo.allRightSize))
	}

	// intersect materialize result row counts
	p.materializeResultCounts = make([]uint64, joinNodeNums)
	addMaterializeResultCount := func(value int) {
		lCount, _ := p.joinInfo.leftSkewData.Get(value)
		rCount, _ := p.joinInfo.rightSkewData.Get(value)
		if p.intersectToLeft {
			for i, _ := range p.joinInfo.joinNodes {
				estimateLeftCount := uint64(lCount) * p.nodeLeftSkewSize[i] / allLeftSkewSize
				estimateRightCount := uint64(rCount)
				p.materializeResultCounts[i] +=  estimateLeftCount * estimateRightCount
			}
		} else {
			for i, _ := range p.joinInfo.joinNodes {
				estimateLeftCount := uint64(lCount)
				estimateRightCount := uint64(rCount) * p.nodeRightSkewSize[i] / allRightSkewSize
				p.materializeResultCounts[i] +=  estimateLeftCount * estimateRightCount
			}
		}
	}
	p.joinInfo.intersect.ForEach(addMaterializeResultCount)
	// left materialize
	if allLeftSkewSize > 0 {
		for _, hv := range p.finalLeftSkewData {
			if p.joinInfo.intersect.Contains(int(hv)) {
				continue
			}
			count, _ := p.joinInfo.leftSkewData.Get(int(hv))

			for i, pIdx := range p.joinInfo.leftRouters {
				proc := p.joinInfo.processors[pIdx]
				nodeID := proc.Node
				bucketIdx := bucketIdxMap[nodeID]
				lCount := uint64(uint64(count) * p.joinInfo.leftRouterSkewSize[i] / allLeftSkewSize)
				rCount := uint64(float64(p.joinInfo.allRightSize) * p.joinInfo.frequencyThreshold)
				p.materializeResultCounts[bucketIdx] += lCount * rCount
			}
		}
	}
	// right materialize
	if allRightSkewSize > 0 {
		for _, hv := range p.finalRightSkewData {
			if p.joinInfo.intersect.Contains(int(hv)) {
				continue
			}
			count, _ := p.joinInfo.rightSkewData.Get(int(hv))

			for i, pIdx := range p.joinInfo.rightRouters {
				proc := p.joinInfo.processors[pIdx]
				nodeID := proc.Node
				bucketIdx := bucketIdxMap[nodeID]
				rCount := uint64(uint64(count) * p.joinInfo.rightRouterSkewSize[i] / allRightSkewSize)
				lCount := uint64(float64(p.joinInfo.allLeftSize) * p.joinInfo.frequencyThreshold)
				p.materializeResultCounts[bucketIdx] += lCount * rCount
			}
		}
	}


	var cost uint64
	for i, _ := range p.joinInfo.joinNodes {
		nodeCost := p.nodeLeftSkewSize[i] + p.nodeRightSkewSize[i] + leftMirrorCount + rightMirrorCount + p.materializeResultCounts[i]
		if cost == 0 || nodeCost > cost {
			cost = nodeCost
		}
	}
	return cost
}

func(p *PRPD) ResultCost() uint64 {
	var cost uint64
	for i, nodeID := range p.joinInfo.joinNodes {
		if nodeID == p.joinInfo.gatewayID {
			continue
		}
		cost += p.materializeResultCounts[i]
	}
	return cost
}

func(p *PRPD) WorkArgs() physicalplan.HashJoinWorkArgs {
	leftHeavyHitters :=	make([]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(p.finalLeftSkewData))
	rightHeavyHitters := make([]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(p.finalRightSkewData))

	for i, value := range p.finalLeftSkewData {
		leftHeavyHitters[i].Value = value
	}
	for i, value := range p.finalRightSkewData {
		rightHeavyHitters[i].Value = value
	}
	workArgs := &physicalplan.PRPDWorkArgs{
		LeftHeavyHitters: leftHeavyHitters,
		RightHeavyHitters: rightHeavyHitters,
	}
	return physicalplan.HashJoinWorkArgs{
		HJType: 			physicalplan.PRPD,
		PRPDArgs: 		workArgs,
	}
}

type PnR struct {
	joinInfo *distJoinHelper

	//finalLeftSkewData  []int64
	//finalRightSkewData []int64
	//finalLeftSkewSize 	int64
	//finalRightSkewSize 	int64

	// left : big table   right : smalle table
	leftLocalSkewData   	[]int64
	leftRandomSkewData 		[]int64
	leftMirrorSkewData 	 	[]int64
	rightRandomSkewData  	[]int64
	rightMirrorSkewData	 	[]int64

	allRandomSkewCounts    uint64
	allMirrorSkewCounts 	 uint64
	allLocalSkewCounts     uint64

	alloc rowenc.DatumAlloc
}

func(pr *PnR) calBucketIdx(key, bucketNums int64) (int, error) {
	encDatum := rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(key)))
	buffer := []byte{}
	var err error
	buffer, err = encDatum.Fingerprint(context.TODO(), types.Int, &pr.alloc, buffer, nil)
	if err != nil {
		return -1, err
	}
	return int(crc32.Update(0, crc32Table, buffer) % uint32(bucketNums)), nil
}

func(pr *PnR) Init() {
	pr.joinInfo.init()

	for _, item := range pr.joinInfo.leftHeavyHitters {
		value := item.Value
		if pr.joinInfo.intersect.Contains(int(value)) {
			continue
		}
		pr.leftLocalSkewData = append(pr.leftLocalSkewData, value)
		pr.rightMirrorSkewData = append(pr.rightMirrorSkewData, value)
		pr.allLocalSkewCounts += uint64(item.Frequency)
	}

	assignIntersect := func (value int) {
		lCount, _ := pr.joinInfo.leftSkewData.Get(value)
		rCount, _ := pr.joinInfo.rightSkewData.Get(value)
		if lCount >= rCount {
			pr.leftRandomSkewData = append(pr.leftRandomSkewData, int64(value))
			pr.rightMirrorSkewData = append(pr.rightMirrorSkewData, int64(value))
		} else {
			pr.rightRandomSkewData = append(pr.rightRandomSkewData, int64(value))
			pr.leftMirrorSkewData = append(pr.leftMirrorSkewData, int64(value))
		}
	}
	pr.joinInfo.intersect.ForEach(assignIntersect)
}

func(pr *PnR) Type() string {
	return "PnR"
}

func(pr *PnR) DistributeCost() uint64 {
	joinNodeNums := uint64(len(pr.joinInfo.joinNodes))

	for _, value := range pr.leftRandomSkewData {
		count, _ := pr.joinInfo.leftSkewData.Get(int(value))
		pr.allRandomSkewCounts += uint64(count)
	}
	for _, value := range pr.rightRandomSkewData {
		count, _ := pr.joinInfo.rightSkewData.Get(int(value))
		pr.allRandomSkewCounts += uint64(count)
	}
	randomSkewCounts := pr.allRandomSkewCounts * (joinNodeNums - 1) / joinNodeNums

	for _, value := range pr.leftMirrorSkewData {
		count, _ := pr.joinInfo.leftSkewData.Get(int(value))
		pr.allMirrorSkewCounts += uint64(count)
	}
	for _, value := range pr.rightMirrorSkewData {
		count, ok := pr.joinInfo.rightSkewData.Get(int(value))
		if ok {
			pr.allMirrorSkewCounts += uint64(count)
			continue
		}
		pr.allMirrorSkewCounts += uint64(float64(pr.joinInfo.allRightSize) * pr.joinInfo.frequencyThreshold)
	}
	mirrorSkewCounts := pr.allMirrorSkewCounts * (joinNodeNums - 1)

	var rightSkewCountsWithoutIntersect uint64
	for _, item := range pr.joinInfo.rightHeavyHitters {
		value := item.Value
		if pr.joinInfo.intersect.Contains(int(value)) {
			continue
		}
		rightSkewCountsWithoutIntersect += uint64(item.Frequency) + uint64(float64(pr.joinInfo.allLeftSize) * pr.joinInfo.frequencyThreshold)
	}
	rightSkewCountsWithoutIntersect = rightSkewCountsWithoutIntersect * (joinNodeNums - 1) / joinNodeNums

	return randomSkewCounts + mirrorSkewCounts + rightSkewCountsWithoutIntersect
}

func(pr *PnR) CalculateCost() uint64 {
	joinNodeNums := uint64(len(pr.joinInfo.joinNodes))

	var allLeftSkewCount uint64
	for _, count := range pr.joinInfo.leftRouterSkewSize {
		allLeftSkewCount += count
	}

	// left computation
	buckets := make([]uint64, joinNodeNums)
	bucketIdxMap := make(map[roachpb.NodeID]int)
	for i, nodeID := range pr.joinInfo.joinNodes {
		bucketIdxMap[nodeID] = i
	}
	if allLeftSkewCount > 0 {
		for i, pIdx := range pr.joinInfo.leftRouters {
			proc := pr.joinInfo.processors[pIdx]
			nodeID := proc.Node
			bucketIdx := bucketIdxMap[nodeID]
			buckets[bucketIdx] += pr.allLocalSkewCounts * pr.joinInfo.leftRouterSkewSize[i] / allLeftSkewCount
		}
	}
	for i, _ := range buckets {
		buckets[i] += pr.allRandomSkewCounts / joinNodeNums + pr.allMirrorSkewCounts
	}
	// right computation
	for _, item := range pr.joinInfo.rightHeavyHitters {
		value := item.Value
		if pr.joinInfo.intersect.Contains(int(value)) {
			continue
		}
		bucketIdx, _ := pr.calBucketIdx(value, int64(joinNodeNums))
		buckets[bucketIdx] += uint64(item.Frequency) + uint64(float64(pr.joinInfo.allLeftSize) * pr.joinInfo.frequencyThreshold)
	}

	// intersect materialize
	var materializeResultCounts uint64
	addComposeResultCount := func(value int) {
		lCount, _ := pr.joinInfo.leftSkewData.Get(value)
		rCount, _ := pr.joinInfo.rightSkewData.Get(value)
		if lCount >= rCount {
			materializeResultCounts += uint64(lCount) / uint64(joinNodeNums) * uint64(rCount)
		} else {
			materializeResultCounts += uint64(rCount) / uint64(joinNodeNums) * uint64(lCount)
		}
	}
	pr.joinInfo.intersect.ForEach(addComposeResultCount)
	for i, _ := range buckets {
		buckets[i] += materializeResultCounts
	}
	// left materialize
	if pr.joinInfo.allLeftSize > 0 {
		for _, value := range pr.leftLocalSkewData {
			count, _ := pr.joinInfo.leftSkewData.Get(int(value))
			skewCount := uint64(count)
			for i, pIdx := range pr.joinInfo.leftRouters {
				proc := pr.joinInfo.processors[pIdx]
				nodeID := proc.Node
				bucketIdx := bucketIdxMap[nodeID]
				lCount := skewCount * pr.joinInfo.leftRouterSkewSize[i] / allLeftSkewCount
				rCount := uint64(float64(pr.joinInfo.allRightSize) * pr.joinInfo.frequencyThreshold)
				buckets[bucketIdx] += lCount * rCount
			}
		}
	}
	// right materialize
	for _, item := range pr.joinInfo.rightHeavyHitters {
		value := item.Value
		if pr.joinInfo.intersect.Contains(int(value)) {
			continue
		}
		bucketIdx, _ := pr.calBucketIdx(value, int64(joinNodeNums))
		rCount := uint64(item.Frequency)
		lCount := uint64(float64(pr.joinInfo.allLeftSize) * pr.joinInfo.frequencyThreshold)
		buckets[bucketIdx] += lCount * rCount
	}


	var cost uint64
	for _, count := range buckets {
		if count > cost {
			cost = count
		}
	}
	return cost
}

func(pr *PnR) ResultCost() uint64 {
	joinNodeNums := uint64(len(pr.joinInfo.joinNodes))
	// each node procedure (nodeSkewCount) counts
	var nodeSkewCount uint64
	addCount := func (value int) {
		lCount, _ := pr.joinInfo.leftSkewData.Get(value)
		rCount, _ := pr.joinInfo.rightSkewData.Get(value)
		if lCount >= rCount {
			nodeSkewCount += uint64(lCount) / uint64(joinNodeNums) * uint64(rCount)
		} else {
			nodeSkewCount += uint64(rCount) / uint64(joinNodeNums) * uint64(lCount)
		}
	}
	pr.joinInfo.intersect.ForEach(addCount)
	return nodeSkewCount * (joinNodeNums - 1)
}

func(pr *PnR) WorkArgs() physicalplan.HashJoinWorkArgs {
	leftLocal := make([]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(pr.leftLocalSkewData))
	leftRandom := make([]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(pr.leftRandomSkewData))
	leftMirror := make([]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(pr.leftMirrorSkewData))
	rightRandom := make([]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(pr.rightRandomSkewData))
	rightMirror := make([]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(pr.rightMirrorSkewData))

	for i, count := range pr.leftLocalSkewData {
		leftLocal[i].Value = int64(count)
	}
	for i, count := range pr.leftRandomSkewData {
		leftRandom[i].Value = int64(count)
	}
	for i, count := range pr.leftMirrorSkewData {
		leftMirror[i].Value = int64(count)
	}
	for i, count := range pr.rightMirrorSkewData {
		rightMirror[i].Value = int64(count)
	}
	for i, count := range pr.rightRandomSkewData {
		rightRandom[i].Value = int64(count)
	}

	workArgs := &physicalplan.PnRWorkArgs{
		LeftRandomHeavyHitters: 	leftRandom,
		LeftLocalHeavyHitters: 		leftLocal,
		LeftMirrorHeavyHitters: 	leftMirror,
		RightMirrorHeavyHitters:  rightMirror,
		RightRandomHeavyHitters: 	rightRandom,
	}
	//leftHeavyHitters :=	make([]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(pr.finalLeftSkewData))
	//rightHeavyHitters := make([]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(pr.finalRightSkewData))
	//
	//for i, value := range pr.finalLeftSkewData {
	//	leftHeavyHitters[i].Value = value
	//}
	//for i, value := range pr.finalRightSkewData {
	//	rightHeavyHitters[i].Value = value
	//}
	//workArgs := &physicalplan.PnRWorkArgs{
	//	LeftHeavyHitters: leftHeavyHitters,
	//	RightHeavyHitters: rightHeavyHitters,
	//}
	return physicalplan.HashJoinWorkArgs{
		HJType: 			physicalplan.PnR,
		PnRArgs: 			workArgs,
	}
}

type BaseHash struct {
	joinInfo *distJoinHelper

	finalLeftSkewData  []int64
	finalRightSkewData []int64

	alloc rowenc.DatumAlloc
}

func(bh *BaseHash) Init() {
	bh.joinInfo.init()
	bh.finalLeftSkewData = make([]int64, 0)
	bh.finalLeftSkewData = make([]int64, 0)
	for _, item := range bh.joinInfo.leftHeavyHitters {
		value := item.Value
		if bh.joinInfo.intersect.Contains(int(value)) {
			continue
		}
		bh.finalLeftSkewData = append(bh.finalLeftSkewData, value)
	}
	for _, item := range bh.joinInfo.rightHeavyHitters {
		value := item.Value
		bh.finalRightSkewData = append(bh.finalRightSkewData, value)
	}

	if bh.joinInfo.intersect.Len() == 1 {
		joinNodeNums := int64(len(bh.joinInfo.joinNodes))
		op := func (value int) {
			bucketIdx, _ := bh.calBucketIdx(int64(value), joinNodeNums)
			selectI := -1
			for i, nodeID := range bh.joinInfo.joinNodes {
				if nodeID == bh.joinInfo.gatewayID {
					selectI = i
					break
				}
			}
			if selectI != -1 {
				bh.joinInfo.joinNodes[selectI] = bh.joinInfo.joinNodes[bucketIdx]
				bh.joinInfo.joinNodes[bucketIdx] = bh.joinInfo.gatewayID
			}
		}
		bh.joinInfo.intersect.ForEach(op)
	}
}

func(bh *BaseHash) Type() string {
	//joinNodeNums := int64(len(bh.joinInfo.joinNodes))
	//log := "basehash skew condition:\n"
	//op := func (value int) {
	//	bucketIdx, _ := bh.calBucketIdx(int64(value), joinNodeNums)
	//	log += "value : " + strconv.FormatInt(int64(value), 10) + ", bucketIdx : " + strconv.FormatInt(int64(bucketIdx), 10) + ", nodeID : " + strconv.FormatInt(int64(bh.joinInfo.joinNodes[bucketIdx]), 10) + "\n"
	//}
	//bh.joinInfo.intersect.ForEach(op)
	//return log + "BaseHash"
	return "BaseHash"
}

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

func(bh *BaseHash) calBucketIdx(key, bucketNums int64) (int, error) {
	encDatum := rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(key)))
	buffer := []byte{}
	var err error
	buffer, err = encDatum.Fingerprint(context.TODO(), types.Int, &bh.alloc, buffer, nil)
	if err != nil {
		return -1, err
	}
	return int(crc32.Update(0, crc32Table, buffer) % uint32(bucketNums)), nil
}

func(bh *BaseHash) DistributeCost() uint64 {
	joinNodeNums := uint64(len(bh.joinInfo.joinNodes))

	var leftCount uint64
	for _, value := range bh.finalLeftSkewData {
		count, _ := bh.joinInfo.leftSkewData.Get(int(value))
		leftCount += uint64(count)

		if bh.joinInfo.intersect.Contains(int(value)) {
			rCount, _ := bh.joinInfo.rightSkewData.Get(int(value))
			leftCount += uint64(rCount)
		} else {
			leftCount += uint64(float64(bh.joinInfo.allRightSize) * bh.joinInfo.frequencyThreshold)
		}
	}

	var rightCount uint64
	for _, value := range bh.finalRightSkewData {
		count, _ := bh.joinInfo.rightSkewData.Get(int(value))
		rightCount += uint64(count)

		if bh.joinInfo.intersect.Contains(int(value)) {
			lCount, _ := bh.joinInfo.leftSkewData.Get(int(value))
			rightCount += uint64(lCount)
		} else {
			rightCount += uint64(float64(bh.joinInfo.allLeftSize) * bh.joinInfo.frequencyThreshold)
		}
	}

	return uint64(leftCount + rightCount) * uint64(joinNodeNums - 1) / joinNodeNums
}

func(bh *BaseHash) CalculateCost() uint64 {
	joinNodeNums := int64(len(bh.joinInfo.joinNodes))
	buckets := make([]uint64, joinNodeNums)

	// right computation
	for _, value := range bh.finalRightSkewData {
		bucketIdx, err := bh.calBucketIdx(value, joinNodeNums)
		if err != nil {
			return 0
		}
		count, _ := bh.joinInfo.rightSkewData.Get(int(value))
		buckets[bucketIdx] += uint64(count)
		if bh.joinInfo.intersect.Contains(int(value)) {
			lCount, _ := bh.joinInfo.leftSkewData.Get(int(value))
			buckets[bucketIdx] += uint64(lCount)
		} else {
			buckets[bucketIdx] += uint64(float64(bh.joinInfo.allLeftSize) * bh.joinInfo.frequencyThreshold)
		}
	}
	// left conputation
	for _, value := range bh.finalLeftSkewData {
		bucketIdx, err := bh.calBucketIdx(value, joinNodeNums)
		if err != nil {
			return 0
		}
		count, _ := bh.joinInfo.leftSkewData.Get(int(value))
		buckets[bucketIdx] += uint64(count)
		if bh.joinInfo.intersect.Contains(int(value)) {
			rCount, _ := bh.joinInfo.rightSkewData.Get(int(value))
			buckets[bucketIdx] += uint64(rCount)
		} else {
			buckets[bucketIdx] += uint64(float64(bh.joinInfo.allRightSize) * bh.joinInfo.frequencyThreshold)
		}
	}

	// intersect materialize
	addComposeResultCount := func(value int) {
		lCount, _ := bh.joinInfo.leftSkewData.Get(value)
		rCount, _ := bh.joinInfo.rightSkewData.Get(value)
		bucketIdx, err := bh.calBucketIdx(int64(value), joinNodeNums)
		if err != nil {
			return
		}
		buckets[bucketIdx] += uint64(lCount) * uint64(rCount)
	}
	bh.joinInfo.intersect.ForEach(addComposeResultCount)
	// left materialize
	for _, value := range bh.finalLeftSkewData {
		if bh.joinInfo.intersect.Contains(int(value)) {
			continue
		}
		bucketIdx, err := bh.calBucketIdx(value, joinNodeNums)
		if err != nil {
			return 0
		}
		count, _ := bh.joinInfo.leftSkewData.Get(int(value))
		lCount := uint64(count)
		rCount := uint64(float64(bh.joinInfo.allRightSize) * bh.joinInfo.frequencyThreshold)
		buckets[bucketIdx] += lCount * rCount
	}
	// right materialize
	for _, value := range bh.finalRightSkewData {
		if bh.joinInfo.intersect.Contains(int(value)) {
			continue
		}
		bucketIdx, err := bh.calBucketIdx(value, joinNodeNums)
		if err != nil {
			return 0
		}
		count, _ := bh.joinInfo.rightSkewData.Get(int(value))
		rCount := uint64(count)
		lCount := uint64(float64(bh.joinInfo.allLeftSize) * bh.joinInfo.frequencyThreshold)
		buckets[bucketIdx] += lCount * rCount
	}


	var cost uint64
	for _, count := range buckets {
		if cost < count {
			cost = count
		}
	}
	return cost
}

func(bh *BaseHash) ResultCost() uint64 {
	joinNodeNums := int64(len(bh.joinInfo.joinNodes))
	var cost uint64
	addCount := func (value int) {
		bucketIdx, err := bh.calBucketIdx(int64(value), joinNodeNums)
		if err != nil || bh.joinInfo.joinNodes[bucketIdx] == bh.joinInfo.gatewayID {
			return
		}
		lCount, _ := bh.joinInfo.leftSkewData.Get(value)
		rCount, _ := bh.joinInfo.rightSkewData.Get(value)
		cost += uint64(lCount) * uint64(rCount)
	}
	bh.joinInfo.intersect.ForEach(addCount)

	return cost
}

func(bh *BaseHash) WorkArgs() physicalplan.HashJoinWorkArgs {
	leftHeavyHitters :=	make([]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(bh.finalLeftSkewData))
	rightHeavyHitters := make([]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(bh.finalRightSkewData))

	for i, value := range bh.finalLeftSkewData {
		leftHeavyHitters[i].Value = value
	}
	for i, value := range bh.finalRightSkewData {
		rightHeavyHitters[i].Value = value
	}
	workArgs := &physicalplan.BaseHashWorkArgs{
		LeftHeavyHitters: leftHeavyHitters,
		RightHeavyHitters: rightHeavyHitters,
	}
	return physicalplan.HashJoinWorkArgs{
		HJType: 			physicalplan.BASEHASH,
		BaseHashArgs: workArgs,
	}
}

func GetHeavyHitters(tableReader *execinfrapb.TableReaderSpec, directory string) []execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter {
	fileName := directory + tableReader.Table.GetName() + ".skew"
	f, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil
	}

	heavyHitters := make([]execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, 0)
	for idx := 0; idx < len(f);  {
		var value int64
		for f[idx] != ',' {
			value = value * 10 + int64(f[idx] - byte('0'))
			idx++
		}
		idx++
		var frequency int64
		for f[idx] != '\n' {
			frequency = frequency * 10 + int64(f[idx] - byte('0'))
			idx++
		}
		idx++

		heavyHitters = append(heavyHitters, execinfrapb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter{
			Value: value,
			Frequency: frequency,
		})
	}

	return heavyHitters
}

func MakeDecisionForHashJoin(
	dsp 								*DistSQLPlanner,
	n										*joinNode,
	processors 					[]physicalplan.Processor,
	leftRouters 				[]physicalplan.ProcessorIdx,
	rightRouters 				[]physicalplan.ProcessorIdx,
	joinNodes						[]roachpb.NodeID,
	getewayID 					roachpb.NodeID,
	frequencyThreshold  float64,
) physicalplan.HashJoinWorkArgs {
	_, leftOk := n.left.plan.(*scanNode)
	_, rightOk := n.right.plan.(*scanNode)
	if !leftOk || !rightOk {
		return physicalplan.HashJoinWorkArgs{
			HJType: physicalplan.BASEHASH,
		}
	}
	leftRowCounts := n.left.plan.(*scanNode).estimatedRowCount
	rightRowCounts := n.right.plan.(*scanNode).estimatedRowCount

	leftTableReader := processors[leftRouters[0]].Spec.Core.TableReader
	rightTableReader := processors[rightRouters[0]].Spec.Core.TableReader
	if leftTableReader.Spans[0].Ranges == nil && rightTableReader.Spans[0].Ranges == nil {
		return physicalplan.HashJoinWorkArgs{
			HJType: physicalplan.BASEHASH,
		}
	}
	user, err := user.Current()
	if err != nil {
		return physicalplan.HashJoinWorkArgs{
			HJType: physicalplan.BASEHASH,
		}
	}
	//directory, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	directory := user.HomeDir
	directory += "/zipf/csv/" + rightTableReader.Table.GetName() + "_" + leftTableReader.Table.GetName() + "/"
	leftHeavyHitters := GetHeavyHitters(leftTableReader, directory)
	rightHeavyHitters := GetHeavyHitters(rightTableReader, directory)
	if leftHeavyHitters == nil && rightHeavyHitters == nil {
		return physicalplan.HashJoinWorkArgs{
			HJType: physicalplan.BASEHASH,
		}
	}

	helper := &distJoinHelper{
		frequencyThreshold: frequencyThreshold,
		leftEstimateRowCounts: leftRowCounts,
		rightEstimateRowCounts: rightRowCounts,
		leftHeavyHitters: 	leftHeavyHitters,
		rightHeavyHitters: 	rightHeavyHitters,
		processors: 		processors,
		leftRouters: 		leftRouters,
		rightRouters: 		rightRouters,
		joinNodes: 			joinNodes,
		gatewayID: 			getewayID,
	}

	if planBaseHash.Get(&dsp.st.SV) {
		return physicalplan.HashJoinWorkArgs{HJType: physicalplan.BASEHASH}
	}
	if planPRPD.Get(&dsp.st.SV) {
		prpd := PRPD{joinInfo: helper}
		prpd.Init()
		return prpd.WorkArgs()
	}
	if planPnR.Get(&dsp.st.SV) {
		pnr := PnR{joinInfo: helper}
		pnr.Init()
		return pnr.WorkArgs()
	}

	// adapte select method
	var costModels []CostModel
	costModels = append(costModels, &BaseHash{joinInfo: helper})
	costModels = append(costModels, &PRPD{joinInfo: helper})
	costModels = append(costModels, &PnR{joinInfo: helper})
	//n.summarized = true

	minCost := uint64(0)
	workArgs := physicalplan.HashJoinWorkArgs{
		HJType: physicalplan.BASEHASH,
	}
	selectType := "BaseHash"
	resultLog := ""
	for _, costModel := range costModels {
		costModel.Init()
		netWork1Cost := costModel.DistributeCost()
		calculateCost := costModel.CalculateCost()
		netWork2Cost := costModel.ResultCost()

		var allCost uint64
		if n.summarized {
			allCost = netWork1Cost + calculateCost + netWork2Cost
		} else {
			allCost = netWork1Cost + calculateCost
		}
		//if minCost == 0 {
		if minCost == 0 || allCost < minCost {
			minCost = allCost
			workArgs = costModel.WorkArgs()
			selectType = costModel.Type()
		}
		resultLog += costModel.Type() + ":\n";
		resultLog += "netWork1Cost : "+ strconv.FormatInt(int64(netWork1Cost), 10) + "\n"
		resultLog += "calculateCost : "+ strconv.FormatInt(int64(calculateCost), 10) + "\n"
		if n.summarized {
			resultLog += "netWork2Cost : "+ strconv.FormatInt(int64(netWork2Cost), 10) + "\n"
		}
		resultLog += "allCost : "+ strconv.FormatInt(int64(allCost), 10) + "\n"
		resultLog += "\n"
	}
	// write result log
	resultLog += "select methods : " + selectType + "\n"
	f, _ := os.OpenFile(directory + "result.log", os.O_CREATE | os.O_RDWR | os.O_TRUNC, 0666)
	io.WriteString(f, resultLog)
	f.Close()

	return workArgs
}
