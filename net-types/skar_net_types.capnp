@0x9289a56a18f880c5;

struct QueryResponseData {
	blocks @0 :Data;
	transactions @1 :Data;
	logs @2 :Data;
}

struct QueryResponse {
	archiveHeight @0 :UInt64;
	nextBlock @1 :UInt64;
	totalExecutionTime @2 :UInt64;
	data @3 :QueryResponseData;
}
