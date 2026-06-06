@TestOn('vm')
library;

import 'package:grpc/src/client/failover/ip_rank.dart';
import 'package:grpc/src/client/failover/probe.dart';
import 'package:test/test.dart';

void main() {
  group('roundBucket', () {
    test('rounds up to bucket size', () {
      expect(roundBucket(1, 10), 10);
      expect(roundBucket(10, 10), 10);
      expect(roundBucket(11, 10), 20);
    });

    test('uses default bucket when zero', () {
      expect(roundBucket(1, 0), 10);
    });
  });

  group('shuffleTied', () {
    test('preserves length and bucket groups', () {
      final ranks = [
        const IpRank(addr: 'a', bucketMs: 10, rawMs: 5),
        const IpRank(addr: 'b', bucketMs: 10, rawMs: 6),
        const IpRank(addr: 'c', bucketMs: 20, rawMs: 15),
      ];
      shuffleTied(ranks, 10);
      expect(ranks.length, 3);
      expect(ranks[0].bucketMs, 10);
      expect(ranks[1].bucketMs, 10);
      expect(ranks[2].bucketMs, 20);
      expect(ranks.map((r) => r.addr).toSet(), {'a', 'b', 'c'});
    });
  });
}
