package config3

// AggregationStep number of transactions in smalest static file
// const HistoryV3AggregationStep = 1_562_500 // = 100M / 64. Dividers: 2, 5, 10, 20, 50, 100, 500
const HistoryV3AggregationStep = 1_562_500 / 10

const EnableHistoryV4InTest = true
