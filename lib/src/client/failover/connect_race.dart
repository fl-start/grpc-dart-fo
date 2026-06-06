// Copyright (c) 2025, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.

import 'dart:async';
import 'dart:io';

import 'config.dart';
import 'ip_rank.dart';
import 'probe.dart';

/// Result of a cold-path TCP connect race.
class TcpRaceResult {
  final Socket? winnerSocket;
  final String winnerAddr;
  final int winnerRawMs;
  final List<IpRank> ranks;

  TcpRaceResult({
    required this.winnerSocket,
    required this.winnerAddr,
    required this.winnerRawMs,
    List<IpRank>? ranks,
  }) : ranks = ranks ?? [];
}

/// Parallel TCP handshake race; first connect wins, losers are destroyed.
Future<TcpRaceResult?> tcpRace(
  String host,
  int port,
  List<String> addrs,
  GrpcFoConfig config,
) async {
  if (addrs.isEmpty) return null;
  if (addrs.length == 1) {
    // ignore: close_sinks — socket is intentionally transferred to TcpRaceResult.winnerSocket.
    final socket = await _connectOne(addrs[0], port, config.connectTimeout);
    if (socket == null) return null;
    return TcpRaceResult(
      winnerSocket: socket,
      winnerAddr: addrs[0],
      winnerRawMs: 0,
      ranks: [IpRank(addr: addrs[0], bucketMs: config.latencyBucketMs, rawMs: 0)],
    );
  }

  if (config.verbose) {
    stderr.writeln(
      'grpc_fo: TCP race $host:$port — ${addrs.length} address(es)',
    );
  }

  final timeout = config.connectTimeout;
  final completer = Completer<TcpRaceResult?>();
  final pending = <_RaceAttempt>[];
  var completed = false;

  for (final addr in addrs) {
    final attempt = _RaceAttempt(addr: addr);
    pending.add(attempt);
    final stopwatch = Stopwatch()..start();
    Socket.connect(InternetAddress(addr), port, timeout: timeout)
        .then((socket) {
      stopwatch.stop();
      if (completed) {
        socket.destroy();
        return;
      }
      completed = true;
      for (final other in pending) {
        if (other.addr != addr && other.socket != null) {
          other.socket!.destroy();
          other.socket = null;
        }
      }
      attempt.socket = socket;
      attempt.rawMs = stopwatch.elapsedMilliseconds;
      if (!completer.isCompleted) {
        completer.complete(
          TcpRaceResult(
            winnerSocket: socket,
            winnerAddr: addr,
            winnerRawMs: attempt.rawMs,
            ranks: [],
          ),
        );
      }
    })
        .catchError((_) {
      attempt.failed = true;
      if (!completed && pending.every((a) => a.failed || a.socket != null)) {
        if (!completer.isCompleted) completer.complete(null);
      }
    });
  }

  final result = await completer.future.timeout(
    timeout + const Duration(milliseconds: 100),
    onTimeout: () {
      if (!completed) {
        for (final a in pending) {
          a.socket?.destroy();
        }
      }
      return null;
    },
  );

  if (result == null) return null;

  // Drain remaining connects in background to build full rank list.
  unawaited(_drainAndBuildRanks(host, port, addrs, pending, config, result));

  return result;
}

/// Fix 8: probe remaining addresses in parallel (Future.wait) instead of
/// sequentially, so that up to 16 addresses each capped at connectTimeout
/// all resolve together rather than one-by-one.
Future<void> _drainAndBuildRanks(
  String host,
  int port,
  List<String> addrs,
  List<_RaceAttempt> pending,
  GrpcFoConfig config,
  TcpRaceResult winnerResult,
) async {
  // Winner latency is already known.
  final winnerRank = IpRank(
    addr: winnerResult.winnerAddr,
    bucketMs: roundBucket(winnerResult.winnerRawMs, config.latencyBucketMs),
    rawMs: winnerResult.winnerRawMs,
  );

  // Collect futures for all non-winner addresses in parallel.
  final futures = <Future<IpRank?>>[];
  for (final addr in addrs) {
    if (addr == winnerResult.winnerAddr) continue;
    final attempt = pending.firstWhere((a) => a.addr == addr);
    if (attempt.failed) continue;
    if (attempt.socket != null && attempt.rawMs > 0) {
      // Already measured during the race.
      futures.add(
        Future.value(
          IpRank(
            addr: addr,
            bucketMs: roundBucket(attempt.rawMs, config.latencyBucketMs),
            rawMs: attempt.rawMs,
          ),
        ),
      );
    } else {
      // In-flight or not-yet-connected: fire a fresh probe connect.
      futures.add(_probeForRank(addr, port, config));
    }
  }

  final probeResults = await Future.wait(futures);

  final ranks = <IpRank>[winnerRank];
  for (final r in probeResults) {
    if (r != null) ranks.add(r);
  }

  ranks.sort((a, b) => a.bucketMs.compareTo(b.bucketMs));
  shuffleTied(ranks, config.latencyBucketMs);
  final n = ranks.length < config.topIps ? ranks.length : config.topIps;
  winnerResult.ranks.addAll(ranks.sublist(0, n));

  if (config.verbose && winnerResult.ranks.isNotEmpty) {
    stderr.writeln('grpc_fo: race ranks committed for $host:$port');
  }
}

Future<IpRank?> _probeForRank(
  String addr,
  int port,
  GrpcFoConfig config,
) async {
  final stopwatch = Stopwatch()..start();
  try {
    final socket = await Socket.connect(
      InternetAddress(addr),
      port,
      timeout: config.connectTimeout,
    );
    stopwatch.stop();
    socket.destroy();
    final rawMs = stopwatch.elapsedMilliseconds;
    return IpRank(
      addr: addr,
      bucketMs: roundBucket(rawMs, config.latencyBucketMs),
      rawMs: rawMs,
    );
  } catch (_) {
    return null;
  }
}

Future<Socket?> _connectOne(String addr, int port, Duration timeout) async {
  try {
    return await Socket.connect(
      InternetAddress(addr),
      port,
      timeout: timeout,
    );
  } catch (_) {
    return null;
  }
}

class _RaceAttempt {
  final String addr;
  Socket? socket;
  int rawMs = 0;
  bool failed = false;

  _RaceAttempt({required this.addr});
}

/// Builds ranks from race drain results for cache commit.
List<IpRank> buildRanksFromRace(
  List<IpRank> ranks,
  GrpcFoConfig config,
) {
  final copy = List<IpRank>.from(ranks);
  copy.sort((a, b) => a.bucketMs.compareTo(b.bucketMs));
  shuffleTied(copy, config.latencyBucketMs);
  final n = copy.length < config.topIps ? copy.length : config.topIps;
  return copy.sublist(0, n);
}

void unawaited(Future<void> future) {
  future.catchError((_) {});
}
