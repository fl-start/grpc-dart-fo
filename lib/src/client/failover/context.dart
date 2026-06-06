// Copyright (c) 2025, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.

import 'cache.dart';
import 'config.dart';
import 'dns.dart';
import 'ip_rank.dart';
import 'resolve_view.dart';

/// Shared context owning the DNS LRU cache (curl_fo cf_ctx).
class GrpcFoContext {
  final GrpcFoConfig config;
  final DnsCache _cache;

  GrpcFoContext(
    this.config, {
    DnsLookupFn? lookup,
  }) : _cache = DnsCache(config, lookup: lookup);

  GrpcFoContext.withDefaults() : this(const GrpcFoConfig());

  /// Resolves [host]:[port] and returns a snapshot safe to use after return.
  Future<ResolveView> resolveSnapshot(String host, int port) {
    return _cache.resolveSnapshot(host, port);
  }

  /// Commits latency ranks from a TCP race/probe background task.
  void commitRanks(String host, int port, List<IpRank> ranks) {
    _cache.commitRanks(host, port, ranks);
  }

  /// Forces re-resolve on next [resolveSnapshot].
  void invalidate(String host, int port) {
    _cache.invalidate(host, port);
  }

  /// Clears all cached entries.
  void clearCache() {
    _cache.clear();
  }
}
