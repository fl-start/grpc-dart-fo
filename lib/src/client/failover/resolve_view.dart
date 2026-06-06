// Copyright (c) 2025, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.

/// Maximum resolved addresses in a snapshot (matches curl_fo CF_RESOLVE_MAX_IPS).
const resolveMaxIps = 16;

/// Read-only view of a resolved host:port for failover connect decisions.
class ResolveView {
  final bool ok;
  final bool multiIp;
  final List<String> ranks;
  final String? singleIp;
  final List<String> allAddrs;

  const ResolveView({
    required this.ok,
    required this.multiIp,
    required this.ranks,
    this.singleIp,
    required this.allAddrs,
  });

  int get rankCount => ranks.length;

  int get allCount => allAddrs.length;

  static const empty = ResolveView(
    ok: false,
    multiIp: false,
    ranks: [],
    allAddrs: [],
  );
}
