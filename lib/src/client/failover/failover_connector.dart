// Copyright (c) 2025, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.

import 'dart:async';
import 'dart:io';

import 'package:http2/transport.dart';

import '../client_transport_connector.dart';
import '../http2_connection.dart';
import '../options.dart';
import '../socket_transport.dart';
import 'config.dart';
import 'connect_race.dart';
import 'context.dart';
import 'ip_rank.dart';
import 'resolve_view.dart';

/// DNS-aware transport connector with curl_fo-style IP failover.
class FailoverTransportConnector implements ClientTransportConnector {
  final String hostname;
  final int port;
  final ChannelOptions options;
  final GrpcFoContext foContext;

  Socket? _socket;
  int _currentRankIndex = 0;
  bool _retriedSameOnReconnect = false;
  bool _hadSuccessfulConnect = false;
  String? _lastConnectedIp;

  FailoverTransportConnector({
    required this.hostname,
    required this.port,
    required this.options,
    required this.foContext,
  }) : assert(options.proxy == null, 'Failover is disabled when proxy is set');

  GrpcFoConfig get config => foContext.config;

  @override
  String get authority => makeAuthority(hostname, port, options);

  @override
  Future<ClientTransportConnection> connect() async {
    Object? lastError;

    final snap = await foContext.resolveSnapshot(hostname, port);
    if (!snap.ok) {
      throw SocketException('DNS resolution failed for $hostname');
    }

    if (!snap.multiIp) {
      return _connectToIp(snap.singleIp!);
    }

    if (_hadSuccessfulConnect && !_retriedSameOnReconnect) {
      _retriedSameOnReconnect = true;
      final ip = _ipAtRank(snap, _currentRankIndex);
      if (ip != null) {
        try {
          return await _connectToIp(ip);
        } catch (e) {
          lastError = e;
          _vlog('reconnect-same $ip failed — trying next ranked IP');
        }
      }
    } else if (_hadSuccessfulConnect) {
      _retriedSameOnReconnect = false;
      _currentRankIndex++;
      _vlog('reconnect-next — advancing to rank index $_currentRankIndex');
    }

    if (snap.rankCount == 0 && config.tcpRace && snap.allCount > 1) {
      try {
        return await _connectColdRace(snap);
      } catch (e) {
        lastError = e;
      }
    }

    final attempts = _rankedAttempts(snap);
    for (var i = 0; i < attempts.length; i++) {
      final ip = attempts[i];
      try {
        _vlog(
          'failover trying $ip (attempt ${i + 1}/${attempts.length})',
        );
        final conn = await _connectToIp(ip);
        _onConnectSuccess(snap, ip);
        return conn;
      } catch (e) {
        lastError = e;
        _vlog('failover $ip failed — ${e.runtimeType}');
      }
    }

    throw lastError ??
        SocketException('All ranked IPs failed for $hostname:$port');
  }

  Future<ClientTransportConnection> _connectColdRace(ResolveView snap) async {
    _vlog('cold-path TCP race for $hostname:$port');
    final race = await tcpRace(hostname, port, snap.allAddrs, config);
    if (race == null || race.winnerSocket == null) {
      throw SocketException('TCP race failed for $hostname:$port');
    }

    _socket = race.winnerSocket;
    try {
      final conn = await connectPinnedSocket(
        race.winnerSocket!,
        hostname,
        port,
        options,
      );
      _onConnectSuccess(snap, race.winnerAddr);
      if (race.ranks.isNotEmpty) {
        foContext.commitRanks(hostname, port, race.ranks);
      } else {
        unawaited(
          _commitRaceRanksWhenReady(hostname, port, snap.allAddrs, race),
        );
      }
      return conn;
    } catch (e) {
      race.winnerSocket?.destroy();
      _socket = null;
      _vlog('race winner ${race.winnerAddr} failed transport — trying siblings');
      Object? lastError = e;
      for (final ip in snap.allAddrs) {
        if (ip == race.winnerAddr) continue;
        try {
          final conn = await _connectToIp(ip);
          _onConnectSuccess(snap, ip);
          return conn;
        } catch (err) {
          lastError = err;
        }
      }
      throw lastError ?? SocketException('TCP race siblings failed for $hostname');
    }
  }

  Future<void> _commitRaceRanksWhenReady(
    String host,
    int p,
    List<String> allAddrs,
    TcpRaceResult race,
  ) async {
    await Future<void>.delayed(config.connectTimeout);
    if (race.ranks.isNotEmpty) {
      foContext.commitRanks(host, p, race.ranks);
      return;
    }
    final ranks = await _probeFallbackRanks(allAddrs);
    if (ranks.isNotEmpty) {
      foContext.commitRanks(host, p, ranks);
    }
  }

  Future<List<IpRank>> _probeFallbackRanks(List<String> addrs) async {
    // Background rank build if race drain did not finish in time.
    return buildRanksFromRace(
      addrs
          .map(
            (a) => IpRank(
              addr: a,
              bucketMs: config.latencyBucketMs,
              rawMs: 0,
            ),
          )
          .toList(),
      config,
    );
  }

  List<String> _rankedAttempts(ResolveView snap) {
    if (snap.rankCount > 0) {
      final start = _currentRankIndex.clamp(0, snap.rankCount - 1);
      return snap.ranks.skip(start).take(config.topIps).toList();
    }
    return snap.allAddrs.take(config.topIps).toList();
  }

  String? _ipAtRank(ResolveView snap, int index) {
    if (snap.rankCount > 0) {
      if (index >= 0 && index < snap.rankCount) return snap.ranks[index];
      return null;
    }
    if (_lastConnectedIp != null) return _lastConnectedIp;
    if (snap.allAddrs.isNotEmpty) return snap.allAddrs[0];
    return null;
  }

  void _onConnectSuccess(ResolveView snap, String ip) {
    _hadSuccessfulConnect = true;
    _lastConnectedIp = ip;
    if (snap.rankCount > 0) {
      final idx = snap.ranks.indexOf(ip);
      if (idx >= 0) _currentRankIndex = idx;
    }
    _retriedSameOnReconnect = false;
    _vlog('connected via $ip');
  }

  Future<ClientTransportConnection> _connectToIp(String ip) async {
    _socket = await openTcpSocket(
      InternetAddress(ip),
      port,
      connectTimeout: options.connectTimeout ?? config.connectTimeout,
    );
    return connectPinnedSocket(_socket!, hostname, port, options);
  }

  void _vlog(String message) {
    if (config.verbose) stderr.writeln('grpc_fo: $message');
  }

  @override
  Future get done {
    if (_socket == null) return Future.value(null);
    return _socket!.done;
  }

  @override
  void shutdown() {
    _socket?.destroy();
    _socket = null;
  }
}

/// Creates a failover connector, or [SocketTransportConnector] when proxy set.
ClientTransportConnector createTransportConnector(
  Object host,
  int port,
  ChannelOptions options, {
  GrpcFoContext? foContext,
}) {
  if (options.proxy != null || foContext == null) {
    return SocketTransportConnector(host, port, options);
  }
  if (host is! String) {
    return SocketTransportConnector(host, port, options);
  }
  return FailoverTransportConnector(
    hostname: host,
    port: port,
    options: options,
    foContext: foContext,
  );
}

void unawaited(Future<void> future) {
  future.catchError((_) {});
}
