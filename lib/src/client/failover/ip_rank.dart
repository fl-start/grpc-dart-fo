// Copyright (c) 2025, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.

/// Latency-ranked IP address (curl_fo cf_ip_rank).
class IpRank {
  final String addr;
  final int bucketMs;
  final int rawMs;

  const IpRank({
    required this.addr,
    required this.bucketMs,
    required this.rawMs,
  });

  static const maxPriority = 0xFFFFFFFF;
}

int roundBucket(int ms, int bucketMs) {
  final bucket = bucketMs == 0 ? 10 : bucketMs;
  final rounded = ((ms + bucket - 1) ~/ bucket) * bucket;
  return rounded == 0 ? bucket : rounded;
}
