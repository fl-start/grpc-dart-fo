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

  /// Completes once [ranks] has been populated by the background drain.
  /// Callers can await this instead of polling.
  final Future<void> ranksReady;

  TcpRaceResult({
    required this.winnerSocket,
    required this.winnerAddr,
    required this.winnerRawMs,
    List<IpRank>? ranks,
    Future<void>? ranksReady,
  })  : ranks = ranks ?? [],
        ranksReady = ranksReady ?? Future<void>.value();
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
  final completer = Completer<_RaceWinner?>();
  final pending = <_RaceAttempt>[];
  var completed = false;

  for (final addr in addrs) {
    final attempt = _RaceAttempt(addr: addr);
    pending.add(attempt);
    final stopwatch = Stopwatch()..start();

    // Each attempt's [settled] future records its measured latency (or
    // failure) exactly once, reusing the single race connect — no duplicate
    // probing later.
    attempt.settled = Socket.connect(InternetAddress(addr), port,
        timeout: timeout)
        .then((socket) {
      stopwatch.stop();
      attempt.latencyMs = stopwatch.elapsedMilliseconds;
      if (!completed) {
        completed = true;
        attempt.socket = socket; // winner keeps its socket
        if (!completer.isCompleted) {
          completer.complete(_RaceWinner(socket, addr, attempt.latencyMs!));
        }
      } else {
        // Loser (or late winner after timeout): keep latency for ranking but
        // close the socket — only the winner's socket is used to connect.
        socket.destroy();
      }
    }).catchError((Object _) {
      attempt.failed = true;
      if (!completed && pending.every((a) => a.failed)) {
        completed = true;
        if (!completer.isCompleted) completer.complete(null);
      }
    });
  }

  final winner = await completer.future.timeout(
    timeout + const Duration(milliseconds: 100),
    onTimeout: () {
      // Fix F: mark completed so any connect that wins *after* the timeout
      // destroys its own socket instead of leaking it.
      completed = true;
      return null;
    },
  );

  if (winner == null) return null;

  // Build the full rank list in the background by awaiting the already
  // running connects (no extra connections), then expose readiness.
  final result = TcpRaceResult(
    winnerSocket: winner.socket,
    winnerAddr: winner.addr,
    winnerRawMs: winner.rawMs,
    ranks: [],
  );
  final ranksReady =
      _drainAndBuildRanks(host, port, pending, config, result);
  return TcpRaceResult(
    winnerSocket: winner.socket,
    winnerAddr: winner.addr,
    winnerRawMs: winner.rawMs,
    ranks: result.ranks,
    ranksReady: ranksReady,
  );
}

/// Awaits the in-flight race connects (no new connections) and fills
/// [winnerResult.ranks] with the measured latency ranking.
Future<void> _drainAndBuildRanks(
  String host,
  int port,
  List<_RaceAttempt> pending,
  GrpcFoConfig config,
  TcpRaceResult winnerResult,
) async {
  // Wait for every connect to resolve or fail (they are already running).
  await Future.wait(pending.map((a) => a.settled));

  final ranks = <IpRank>[];
  for (final a in pending) {
    final ms = a.latencyMs;
    if (ms != null) {
      ranks.add(
        IpRank(
          addr: a.addr,
          bucketMs: roundBucket(ms, config.latencyBucketMs),
          rawMs: ms,
        ),
      );
    }
  }

  ranks.sort((a, b) => a.bucketMs.compareTo(b.bucketMs));
  shuffleTied(ranks, config.latencyBucketMs);
  final n = ranks.length < config.topIps ? ranks.length : config.topIps;
  winnerResult.ranks.addAll(ranks.sublist(0, n));

  if (config.verbose && winnerResult.ranks.isNotEmpty) {
    stderr.writeln('grpc_fo: race ranks committed for $host:$port');
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

class _RaceWinner {
  final Socket socket;
  final String addr;
  final int rawMs;

  _RaceWinner(this.socket, this.addr, this.rawMs);
}

class _RaceAttempt {
  final String addr;
  Socket? socket;
  int? latencyMs;
  bool failed = false;
  Future<void> settled = Future<void>.value();

  _RaceAttempt({required this.addr});
}

void unawaited(Future<void> future) {
  future.catchError((_) {});
}
