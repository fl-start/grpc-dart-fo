// Copyright (c) 2025, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.

import 'dart:async';
import 'dart:io';

import 'config.dart';
import 'dns.dart';
import 'ip_rank.dart';
import 'probe.dart';
import 'resolve_view.dart';

/// Cached DNS entry for a host:port pair (curl_fo cf_dns_entry).
class DnsCacheEntry {
  final String host;
  final int port;
  List<IpRank> ranks;
  List<String> allAddrs;
  bool multiIp;
  bool probed;
  int ttlSec;
  DateTime expiresAt;
  bool refreshing;

  DnsCacheEntry({
    required this.host,
    required this.port,
    this.ranks = const [],
    this.allAddrs = const [],
    this.multiIp = false,
    this.probed = false,
    this.ttlSec = 0,
    required this.expiresAt,
    this.refreshing = false,
  });
}

/// LRU DNS cache with TTL refresh (curl_fo cf_cache).
class DnsCache {
  final GrpcFoConfig config;
  final DnsLookupFn? lookup;
  final Map<String, DnsCacheEntry> _entries = {};
  final List<String> _lruKeys = [];

  DnsCache(this.config, {this.lookup});

  String _key(String host, int port) => '$host:$port';

  void _touch(String key) {
    _lruKeys.remove(key);
    _lruKeys.insert(0, key);
  }

  void _evictIfNeeded() {
    while (_entries.length > config.lruCapacity && _lruKeys.isNotEmpty) {
      final evictKey = _lruKeys.removeLast();
      _entries.remove(evictKey);
    }
  }

  DnsCacheEntry? _find(String host, int port) =>
      _entries[_key(host, port)];

  ResolveView _snapshotFromEntry(DnsCacheEntry entry) {
    if (!entry.multiIp && entry.allAddrs.length == 1) {
      return ResolveView(
        ok: true,
        multiIp: false,
        ranks: const [],
        singleIp: entry.allAddrs[0],
        allAddrs: List.unmodifiable(entry.allAddrs),
      );
    }
    return ResolveView(
      ok: true,
      multiIp: entry.multiIp,
      ranks: entry.ranks.map((r) => r.addr).toList(),
      allAddrs: List.unmodifiable(entry.allAddrs),
    );
  }

  void _vlog(String message) {
    if (config.verbose) stderr.writeln('grpc_fo: $message');
  }

  Future<ResolveView> resolveSnapshot(String host, int port) async {
    final key = _key(host, port);
    final now = DateTime.now();
    final entry = _find(host, port);

    if (entry != null && now.isBefore(entry.expiresAt)) {
      _vlog(
        'cache hit $host:$port (${entry.allAddrs.length} IP(s), '
        '${entry.ranks.length} ranked)',
      );
      _touch(key);
      return _snapshotFromEntry(entry);
    }

    if (entry != null) {
      if (entry.refreshing) {
        return _snapshotFromEntry(entry);
      }
      entry.refreshing = true;
      _vlog('TTL expired for $host:$port — refreshing');
      try {
        await _refreshExpired(entry);
      } finally {
        entry.refreshing = false;
      }
      _touch(key);
      return _snapshotFromEntry(entry);
    }

    _vlog('cache miss $host:$port — resolving');
    final fresh = await _populate(host, port);
    final existing = _find(host, port);
    if (existing != null) {
      _touch(key);
      return _snapshotFromEntry(existing);
    }

    _entries[key] = fresh;
    _touch(key);
    _evictIfNeeded();
    return _snapshotFromEntry(fresh);
  }

  Future<DnsCacheEntry> _populate(String host, int port) async {
    final res = await dnsResolve(
      host,
      config.defaultTtlSec,
      lookup: lookup,
    );
    final now = DateTime.now();
    final entry = DnsCacheEntry(
      host: host,
      port: port,
      allAddrs: res.addrs,
      ttlSec: res.ttlSec,
      expiresAt: now.add(Duration(seconds: res.ttlSec)),
      multiIp: res.addrs.length > 1,
      probed: false,
      ranks: const [],
    );

    if (!entry.multiIp) {
      _vlog('single IP ${res.addrs[0]} — failover not needed');
    } else {
      _vlog(
        'multi-IP $host:$port — ${res.addrs.length} address(es), '
        'TCP race on first connect',
      );
    }
    return entry;
  }

  /// Returns true if full re-probe is required.
  Future<bool> _refreshTtl(DnsCacheEntry entry) async {
    final res = await dnsResolve(
      entry.host,
      config.defaultTtlSec,
      lookup: lookup,
    );

    final topIp = entry.ranks.isNotEmpty ? entry.ranks[0].addr : null;
    final topStillPresent =
        topIp != null && addrInList(topIp, res.addrs);

    entry.allAddrs = res.addrs;
    entry.ttlSec = res.ttlSec;
    entry.multiIp = res.addrs.length > 1;

    if (!entry.multiIp) {
      entry.rankCountReset();
      return false;
    }

    if (!entry.probed || !topStillPresent) {
      return true;
    }

    final newRanks = <IpRank>[];
    for (final rank in entry.ranks) {
      if (addrInList(rank.addr, res.addrs)) {
        newRanks.add(rank);
      }
    }
    for (final addr in res.addrs) {
      if (!newRanks.any((r) => r.addr == addr)) {
        newRanks.add(
          IpRank(
            addr: addr,
            bucketMs: IpRank.maxPriority,
            rawMs: IpRank.maxPriority,
          ),
        );
      }
    }

    final topN = config.topIps;
    entry.ranks = newRanks.length > topN
        ? newRanks.sublist(0, topN)
        : newRanks;
    return false;
  }

  Future<void> _refreshExpired(DnsCacheEntry entry) async {
    final needProbe = await _refreshTtl(entry);
    if (needProbe) {
      _vlog('top ranked IP gone — re-probing ${entry.host}:${entry.port}');
      await _reprobe(entry);
    }
    entry.expiresAt = DateTime.now().add(Duration(seconds: entry.ttlSec));
  }

  Future<void> _reprobe(DnsCacheEntry entry) async {
    entry.ranks = [];
    entry.probed = false;
    final ranks = await probeRank(
      entry.host,
      entry.port,
      entry.allAddrs,
      config,
    );
    if (ranks.isNotEmpty) {
      entry.ranks = ranks;
      entry.probed = true;
    }
  }

  void commitRanks(String host, int port, List<IpRank> ranks) {
    if (ranks.isEmpty) return;
    final entry = _find(host, port);
    if (entry == null) return;
    entry.ranks = ranks;
    entry.probed = true;
    _vlog('cache ranks committed for $host:$port (${ranks.length} IP(s))');
  }

  void invalidate(String host, int port) {
    final key = _key(host, port);
    _entries.remove(key);
    _lruKeys.remove(key);
  }

  void clear() {
    _entries.clear();
    _lruKeys.clear();
  }
}

extension on DnsCacheEntry {
  void rankCountReset() {
    ranks = [];
    probed = false;
  }
}
