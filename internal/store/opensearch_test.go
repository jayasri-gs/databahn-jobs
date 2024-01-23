package store

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestJson(t *testing.T) {
	j := `{
    "took": 614,
    "timed_out": false,
    "_shards": {
        "total": 5,
        "successful": 5,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 10000,
            "relation": "gte"
        },
        "max_score": null,
        "hits": []
    },
    "aggregations": {
        "group_by": {
            "after_key": {
                "key1.keyword": "device_26",
                "key2.keyword": "",
                "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                "day_end_timestamp": 1705343399999
            },
            "buckets": [
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 909
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1212
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1313
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1111
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1212
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1212
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1212
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1212
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1212
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1212
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1212
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1212
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1313
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1809
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2412
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2412
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2412
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2412
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2613
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2211
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2412
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2412
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2412
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2412
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2412
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_1",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2412
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 990
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1320
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1320
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1320
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1320
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1320
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1320
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1320
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1320
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1320
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1430
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1210
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1320
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1890
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2520
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2520
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2520
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2520
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2520
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2520
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2520
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2520
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2520
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2730
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2310
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_10",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2520
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1800
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2400
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2400
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2400
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2400
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2400
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2400
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2400
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2400
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2400
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2400
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2400
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2600
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 2700
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 3600
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 3900
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 3300
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 3600
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 3600
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 3600
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 3600
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 3900
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 3300
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 3600
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 3600
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_100",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 3600
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 999
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1332
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1332
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1332
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1332
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1443
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1221
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1332
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1332
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1332
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1332
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1332
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1332
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1899
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2743
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2321
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2743
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2321
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2743
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2321
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2532
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2532
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2532
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2532
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2532
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_11",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2532
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1008
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1344
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1344
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1344
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1344
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1344
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1344
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1344
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1344
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1344
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1344
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1344
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1344
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1908
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2544
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2544
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2544
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2544
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2544
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2544
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2544
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2544
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2544
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2544
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2544
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_12",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2544
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1017
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1356
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1469
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1243
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1356
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1356
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1469
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1243
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1356
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1356
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1356
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1356
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1356
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1917
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2556
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2556
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2556
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2556
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2556
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2556
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2556
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2556
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2556
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2556
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2556
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_13",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2556
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 10,
                    "sum_count": {
                        "value": 1140
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1254
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1368
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1368
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1368
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1368
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1368
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1368
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1368
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1368
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1368
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1368
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1368
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1926
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2568
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2568
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2568
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2568
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2568
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2568
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2568
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2568
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2568
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2568
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2568
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_14",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2568
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 10,
                    "sum_count": {
                        "value": 1150
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1265
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1380
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1380
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1380
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1380
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1380
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1380
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1380
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1380
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1380
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1380
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1380
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1935
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2580
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2580
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2580
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2580
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2580
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2580
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2580
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2580
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2580
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2580
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2580
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_15",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2580
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1044
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1392
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1392
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1392
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1392
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1392
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1392
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1392
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1392
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1392
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1392
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1392
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1392
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1944
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2592
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2592
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2592
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2592
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2808
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2376
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2592
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2592
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2592
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2592
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2592
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_16",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2592
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1053
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1404
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1404
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1404
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1404
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1404
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1404
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1404
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1404
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1404
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1404
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1404
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1404
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1953
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2604
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2604
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2604
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2604
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2604
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2604
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2604
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2604
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2604
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2604
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2821
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_17",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2387
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1062
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1416
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1416
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1416
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1416
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1416
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1416
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1416
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1416
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1416
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1534
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1298
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1416
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1962
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2616
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2616
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2616
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2616
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2616
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2616
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2616
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2616
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2616
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2616
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2616
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_18",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2834
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 10,
                    "sum_count": {
                        "value": 1190
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1428
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1309
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1547
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1309
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1547
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1309
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1428
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1428
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1428
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1547
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1309
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1428
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1971
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2628
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2847
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2409
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2628
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2628
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2628
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2847
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2409
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2628
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2628
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2628
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_19",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2628
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 918
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1224
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1224
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1224
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1224
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1224
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1224
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1224
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1224
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1224
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1224
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1224
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1224
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1818
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2424
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2424
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2424
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2424
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2424
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2424
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2424
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2424
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2424
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2424
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2424
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_2",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2424
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1080
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1440
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1440
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1440
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1440
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1440
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1440
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1440
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1440
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1440
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1440
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1440
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1440
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1980
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2640
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2640
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2640
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2640
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2640
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2640
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2860
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2420
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2640
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2640
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2640
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_20",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2640
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1089
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1452
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1452
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1573
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1331
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1452
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1452
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1452
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1452
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1452
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1452
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1452
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1452
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1989
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2652
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2652
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2652
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2652
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2652
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2652
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2652
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2652
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2652
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2873
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2431
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_21",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2652
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1098
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1464
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1464
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1464
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1464
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1464
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1464
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1464
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1464
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1464
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1464
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1464
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1464
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1998
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2664
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2664
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2664
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2664
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2664
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2664
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2664
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2664
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2664
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2664
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2664
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_22",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2664
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1107
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1476
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1476
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1476
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1476
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1476
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1476
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1476
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1476
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1599
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1353
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1476
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1476
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 2007
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2676
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2676
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2676
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2676
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2676
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2676
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2676
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2676
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2899
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2453
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2676
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_23",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2676
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1116
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1612
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1364
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1488
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1488
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1488
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1488
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1488
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1488
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1488
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1488
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1488
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1488
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 2016
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2688
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2688
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2688
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2688
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2688
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2688
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2688
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2688
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2688
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2688
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2688
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_24",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2688
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1125
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1500
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1500
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1500
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1500
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1625
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1375
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1500
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 1625
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1500
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 1375
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1500
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1500
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 2025
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2925
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2475
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2700
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2700
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2700
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705429799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2700
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705516199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2700
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705602599999
                    },
                    "doc_count": 13,
                    "sum_count": {
                        "value": 2925
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705688999999
                    },
                    "doc_count": 11,
                    "sum_count": {
                        "value": 2475
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705775399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2700
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705861799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2700
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_25",
                        "key2.keyword": "",
                        "source_id": "2222c1d2-ca31-42f6-a900-4a9062442222",
                        "day_end_timestamp": 1705948199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 2700
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_26",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704911399999
                    },
                    "doc_count": 9,
                    "sum_count": {
                        "value": 1134
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_26",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1704997799999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1512
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_26",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705084199999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1512
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_26",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705170599999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1512
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_26",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705256999999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1512
                    }
                },
                {
                    "key": {
                        "key1.keyword": "device_26",
                        "key2.keyword": "",
                        "source_id": "1111c1d2-ca31-42f6-a900-4a9062441111",
                        "day_end_timestamp": 1705343399999
                    },
                    "doc_count": 12,
                    "sum_count": {
                        "value": 1512
                    }
                }
            ]
        }
    }
}`
	aggResponse := &CompositeAggregationResponse{}
	err := json.Unmarshal([]byte(j), aggResponse)
	if err != nil {
		panic(err)
	}
	fmt.Println(aggResponse)
}
