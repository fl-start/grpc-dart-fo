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

    // Fix 5: call _onConnectSuccess on single-IP path so state is consistent
    // if DNS later refreshes to multi-IP.
    if (!snap.multiIp) {
      final conn = await _connectToIp(snap.singleIp!);
      _onConnectSuccess(snap, snap.singleIp!);
      return conn;
    }

    // Fix 2: cap and wrap _currentRankIndex so it never stays past the last IP.
    if (snap.rankCount > 0 && _currentRankIndex >= snap.rankCount) {
      _currentRankIndex = 0;
    }

    if (_hadSuccessfulConnect && !_retriedSameOnReconnect) {
      // Reconnect policy (curl_fo WS): retry same IP once before advancing.
      _retriedSameOnReconnect = true;
      final ip = _ipAtRank(snap, _currentRankIndex);
      if (ip != null) {
        try {
          final conn = await _connectToIp(ip);
          _onConnectSuccess(snap, ip);
          return conn;
        } catch (e) {
          lastError = e;
          _vlog('reconnect-same $ip failed — trying next ranked IP');
        }
      }
    } else if (_hadSuccessfulConnect) {
      _retriedSameOnReconnect = false;
      _currentRankIndex =
          snap.rankCount > 0 ? (_currentRankIndex + 1) % snap.rankCount : 0;
      _vlog('reconnect-next — advancing to rank index $_currentRankIndex');
    }

    // Fix 3: track whether cold race was attempted so we don't retry the
    // same allAddrs again in the ranked loop below.
    var coldRaceAttempted = false;
    if (snap.rankCount == 0 && config.tcpRace && snap.allCount > 1) {
      coldRaceAttempted = true;
      try {
        return await _connectColdRace(snap);
      } catch (e) {
        lastError = e;
        // Cold race exhausted all addresses (winner + siblings). Do not fall
        // through to another allAddrs retry — re-throw immediately.
        throw lastError;
      }
    }

    final attempts = _rankedAttempts(snap, skipColdAddrs: coldRaceAttempted);
    for (var i = 0; i < attempts.length; i++) {
      final ip = attempts[i];
      try {
        _vlog('failover trying $ip (attempt ${i + 1}/${attempts.length})');
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
      final connected = await connectPinnedSocket(
        race.winnerSocket!,
        hostname,
        port,
        options,
      );
      // Track the final (possibly TLS) socket for done/shutdown.
      _socket = connected.socket;
      final conn = connected.connection;
      _onConnectSuccess(snap, race.winnerAddr);
      if (race.ranks.isNotEmpty) {
        foContext.commitRanks(hostname, port, race.ranks);
      } else {
        // Background drain to commit real latency ranks once available.
        unawaited(_awaitAndCommitRaceRanks(hostname, port, race));
      }
      return conn;
    } catch (e) {
      // Fix 1: destroy the winner socket explicitly on TLS/HTTP2 failure.
      race.winnerSocket?.destroy();
      _socket = null;
      _vlog(
        'race winner ${race.winnerAddr} failed transport — trying siblings',
      );
      Object? lastError = e;
      for (final ip in snap.allAddrs) {
        if (ip == race.winnerAddr) continue;
        try {
          // Fix 1: _connectToIp handles socket leak internally.
          final conn = await _connectToIp(ip);
          _onConnectSuccess(snap, ip);
          return conn;
        } catch (err) {
          lastError = err;
        }
      }
      throw lastError ??
          SocketException('TCP race siblings failed for $hostname');
    }
  }

  /// Awaits the background drain (no polling) and commits the resulting
  /// ranks. Never commits fake-latency data — if the drain produced no
  /// measurements nothing is committed, leaving probed=false so the next
  /// TTL refresh triggers a real re-probe.
  Future<void> _awaitAndCommitRaceRanks(
    String host,
    int p,
    TcpRaceResult race,
  ) async {
    await race.ranksReady;
    if (race.ranks.isNotEmpty) {
      foContext.commitRanks(host, p, race.ranks);
    }
  }

  List<String> _rankedAttempts(
    ResolveView snap, {
    bool skipColdAddrs = false,
  }) {
    if (snap.rankCount > 0) {
      final start = _currentRankIndex.clamp(0, snap.rankCount - 1);
      return snap.ranks.skip(start).take(config.topIps).toList();
    }
    // Fix 3: if cold race was NOT attempted, use allAddrs; otherwise empty
    // (cold race already tried every address).
    if (skipColdAddrs) return const [];
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

  /// Fix 1: Opens a TCP socket, upgrades to TLS, and wraps as HTTP/2 transport.
  /// If TLS or HTTP/2 setup fails the raw TCP socket is destroyed before
  /// re-throwing so no file descriptor is leaked.
  Future<ClientTransportConnection> _connectToIp(String ip) async {
    final socket = await openTcpSocket(
      InternetAddress(ip),
      port,
      connectTimeout: options.connectTimeout ?? config.connectTimeout,
    );
    _socket = socket;
    try {
      final connected = await connectPinnedSocket(
        socket,
        hostname,
        port,
        options,
      );
      // Track the final (possibly TLS) socket for done/shutdown.
      _socket = connected.socket;
      return connected.connection;
    } catch (_) {
      socket.destroy();
      _socket = null;
      rethrow;
    }
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
