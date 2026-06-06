// Copyright (c) 2025, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.

import 'dart:io';

import 'resolve_view.dart';

/// Result of a DNS lookup (curl_fo cf_dns_result fallback path).
class DnsResult {
  final List<String> addrs;
  final int ttlSec;

  const DnsResult({required this.addrs, required this.ttlSec});
}

/// Resolves A + AAAA via [InternetAddress.lookup], deduped and capped.
typedef DnsLookupFn = Future<List<InternetAddress>> Function(String host);

Future<List<InternetAddress>> defaultDnsLookup(String host) {
  return InternetAddress.lookup(host, type: InternetAddressType.any);
}

Future<DnsResult> dnsResolve(
  String host,
  int defaultTtlSec, {
  DnsLookupFn? lookup,
}) async {
  final lookupFn = lookup ?? defaultDnsLookup;
  final addresses = await lookupFn(host);

  final seen = <String>{};
  final addrs = <String>[];
  for (final addr in addresses) {
    final ip = addr.address;
    if (seen.add(ip)) {
      addrs.add(ip);
      if (addrs.length >= resolveMaxIps) break;
    }
  }

  if (addrs.isEmpty) {
    throw SocketException('Failed host lookup: $host');
  }

  return DnsResult(addrs: addrs, ttlSec: defaultTtlSec);
}

bool addrInList(String addr, List<String> list) => list.contains(addr);
