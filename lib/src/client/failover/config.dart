// Copyright (c) 2025, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.

/// Configuration for DNS-aware IP failover (curl_fo strategy).
class GrpcFoConfig {
  /// Maximum cached host:port entries.
  final int lruCapacity;

  /// Maximum ranked IPs used for failover attempts.
  final int topIps;

  /// Per-attempt connect timeout for probes and races.
  final Duration connectTimeout;

  /// Latency rounding bucket in milliseconds.
  final int latencyBucketMs;

  /// Cache TTL when DNS TTL is unavailable (InternetAddress.lookup path).
  final int defaultTtlSec;

  /// Parallel TCP handshake race on cold multi-IP connects.
  final bool tcpRace;

  /// Log failover activity to stderr.
  final bool verbose;

  const GrpcFoConfig({
    this.lruCapacity = 500,
    this.topIps = 3,
    this.connectTimeout = const Duration(seconds: 3),
    this.latencyBucketMs = 10,
    this.defaultTtlSec = 300,
    this.tcpRace = true,
    this.verbose = false,
  });

  GrpcFoConfig copyWith({
    int? lruCapacity,
    int? topIps,
    Duration? connectTimeout,
    int? latencyBucketMs,
    int? defaultTtlSec,
    bool? tcpRace,
    bool? verbose,
  }) {
    return GrpcFoConfig(
      lruCapacity: lruCapacity ?? this.lruCapacity,
      topIps: topIps ?? this.topIps,
      connectTimeout: connectTimeout ?? this.connectTimeout,
      latencyBucketMs: latencyBucketMs ?? this.latencyBucketMs,
      defaultTtlSec: defaultTtlSec ?? this.defaultTtlSec,
      tcpRace: tcpRace ?? this.tcpRace,
      verbose: verbose ?? this.verbose,
    );
  }
}
