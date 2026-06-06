@TestOn('vm')
library;

import 'dart:io';

import 'package:grpc/src/client/failover/dns.dart';
import 'package:grpc/src/client/failover/resolve_view.dart';
import 'package:test/test.dart';

Future<List<InternetAddress>> _fakeLookup(String host) async {
  return [
    InternetAddress('127.0.0.1'),
    InternetAddress('127.0.0.1'), // duplicate
    InternetAddress('::1'),
  ];
}

void main() {
  group('dnsResolve', () {
    test('dedupes addresses and merges v4/v6', () async {
      final result = await dnsResolve(
        'example.test',
        300,
        lookup: _fakeLookup,
      );
      expect(result.addrs, ['127.0.0.1', '::1']);
      expect(result.ttlSec, 300);
    });

    test('caps at resolveMaxIps', () async {
      Future<List<InternetAddress>> manyLookup(String host) async {
        return List.generate(
          20,
          (i) => InternetAddress('127.0.0.${i + 1}'),
        );
      }

      final result = await dnsResolve(
        'many.test',
        300,
        lookup: manyLookup,
      );
      expect(result.addrs.length, resolveMaxIps);
    });

    test('throws when lookup returns empty', () async {
      expect(
        () => dnsResolve('empty.test', 300, lookup: (_) async => []),
        throwsA(isA<SocketException>()),
      );
    });
  });
}
