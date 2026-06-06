@TestOn('vm')
library;

import 'dart:io';

import 'package:grpc/src/client/failover/config.dart';
import 'package:grpc/src/client/failover/connect_race.dart';
import 'package:test/test.dart';

void main() {
  group('tcpRace', () {
    late ServerSocket server;
    late int port;

    setUp(() async {
      server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      port = server.port;
      server.listen((socket) {
        // Keep connection open briefly.
        socket.listen((_) {}, onDone: () => socket.destroy());
      });
    });

    tearDown(() async {
      await server.close();
    });

    test('returns winner for reachable address', () async {
      final result = await tcpRace(
        'race.test',
        port,
        ['127.0.0.1'],
        const GrpcFoConfig(connectTimeout: Duration(seconds: 2)),
      );

      expect(result, isNotNull);
      expect(result!.winnerAddr, '127.0.0.1');
      expect(result.winnerSocket, isNotNull);
      result.winnerSocket!.destroy();
    });

    test('returns null when all addresses fail', () async {
      final result = await tcpRace(
        'race.test',
        port,
        ['127.0.0.255'],
        const GrpcFoConfig(
          connectTimeout: Duration(milliseconds: 200),
          tcpRace: true,
        ),
      );
      expect(result, isNull);
    });
  });
}
