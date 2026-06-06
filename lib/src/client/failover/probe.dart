// Copyright (c) 2025, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.

import 'dart:async';
import 'dart:io';
import 'dart:math';

import 'config.dart';
import 'ip_rank.dart';

final _random = Random();

/// Shuffles [ranks] within contiguous groups sharing the same [bucketMs].
void shuffleTied(List<IpRank> ranks, int bucketMs) {
  var i = 0;
  while (i < ranks.length) {
    var j = i + 1;
    while (j < ranks.length && ranks[j].bucketMs == ranks[i].bucketMs) {
      j++;
    }
    if (j - i > 1) {
      final slice = ranks.sublist(i, j);
      slice.shuffle(_random);
      for (var k = 0; k < slice.length; k++) {
        ranks[i + k] = slice[k];
      }
    }
    i = j;
  }
}

/// Measures TCP connect latency to each address and returns latency-ranked IPs.
Future<List<IpRank>> probeRank(
  String host,
  int port,
  List<String> addrs,
  GrpcFoConfig config,
) async {
  if (addrs.isEmpty) return [];

  final timeout = config.connectTimeout;
  final bucketMs = config.latencyBucketMs;
  final topN = config.topIps;

  if (config.verbose) {
    stderr.writeln(
      'grpc_fo: probing $host:$port — ${addrs.length} address(es), '
      'bucket=${bucketMs}ms',
    );
  }

  final results = await Future.wait(
    addrs.map((addr) => _probeOne(addr, port, timeout, bucketMs, config)),
  );

  final ranks = results.whereType<IpRank>().toList();
  if (ranks.isEmpty) return [];

  ranks.sort((a, b) => a.bucketMs.compareTo(b.bucketMs));
  shuffleTied(ranks, bucketMs);

  final n = ranks.length < topN ? ranks.length : topN;
  final out = ranks.sublist(0, n);

  if (config.verbose) {
    for (var i = 0; i < out.length; i++) {
      stderr.writeln(
        'grpc_fo:   rank[${i + 1}] ${out[i].addr} '
        '(${out[i].rawMs}ms, bucket=${out[i].bucketMs}ms)',
      );
    }
  }

  return out;
}

Future<IpRank?> _probeOne(
  String addr,
  int port,
  Duration timeout,
  int bucketMs,
  GrpcFoConfig config,
) async {
  final stopwatch = Stopwatch()..start();
  Socket? socket;
  try {
    socket = await Socket.connect(
      InternetAddress(addr),
      port,
      timeout: timeout,
    );
    stopwatch.stop();
    final rawMs = stopwatch.elapsedMilliseconds;
    final bucket = roundBucket(rawMs, bucketMs);
    if (config.verbose) {
      stderr.writeln('grpc_fo:   probe $addr ok ${rawMs}ms (bucket=$bucket ms)');
    }
    return IpRank(addr: addr, bucketMs: bucket, rawMs: rawMs);
  } catch (_) {
    if (config.verbose) {
      stderr.writeln('grpc_fo:   probe $addr failed');
    }
    return null;
  } finally {
    socket?.destroy();
  }
}
